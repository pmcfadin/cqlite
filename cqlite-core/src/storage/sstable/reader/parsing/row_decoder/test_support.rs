//! Shared test fixtures/helpers for the row_decoder parser tests.
#![allow(dead_code)]

use crate::parser::vint::parse_vuint;
use crate::{Error, Result};

pub(crate) mod helpers {
    /// Local VInt encoder for test helpers — avoids depending on
    /// `storage::serialization` which is gated behind `write-support`.
    /// Byte-identical to Cassandra's writeUnsignedVInt / VIntCoding.java.
    pub(crate) fn encode_unsigned(value: u64, buf: &mut Vec<u8>) {
        // Compute byte count using Cassandra's formula:
        //   size = (639 - leading_zeros(value | 1) * 9) >> 6
        let magnitude = (value | 1).leading_zeros();
        let size = ((639 - magnitude * 9) >> 6) as usize;

        if size == 1 {
            buf.push(value as u8);
        } else if size == 9 {
            buf.push(0xFF);
            buf.extend_from_slice(&value.to_be_bytes());
        } else {
            let extra_bytes = size - 1;
            let shift = 8usize.saturating_sub(extra_bytes);
            let mask: u8 = if extra_bytes == 0 {
                0x00
            } else if extra_bytes >= 8 {
                0xFF
            } else {
                0xFF_u8 << shift
            };
            let first_byte_data_bits = 8 - extra_bytes - 1;
            let data_shift = extra_bytes * 8;
            let first_byte_data = ((value >> data_shift) & ((1 << first_byte_data_bits) - 1)) as u8;
            buf.push(mask | first_byte_data);
            for i in (0..extra_bytes).rev() {
                buf.push(((value >> (i * 8)) & 0xFF) as u8);
            }
        }
    }

    /// Helper: build a frozen list<int> raw binary: [i32 count][i32 len][int]...
    pub(crate) fn build_frozen_list_int(values: &[i32]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as i32).to_be_bytes());
        for &v in values {
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }
        buf
    }

    /// Helper: build a frozen map<text,int> raw binary
    pub(crate) fn build_frozen_map_text_int(entries: &[(&str, i32)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
        for &(k, v) in entries {
            let k_bytes = k.as_bytes();
            buf.extend_from_slice(&(k_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(k_bytes);
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }
        buf
    }

    /// Helper: build a frozen list<text> raw binary in the same
    /// `[i32 BE count]` + per-element `[i32 BE len][utf8 bytes]` framing that
    /// `parse_frozen_list_value_raw` expects for an already-bounded element.
    pub(crate) fn build_frozen_list_text(values: &[&str]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as i32).to_be_bytes());
        for v in values {
            let b = v.as_bytes();
            buf.extend_from_slice(&(b.len() as i32).to_be_bytes());
            buf.extend_from_slice(b);
        }
        buf
    }

    // -----------------------------------------------------------------------
    // Regression tests for Issue #481
    // -----------------------------------------------------------------------

    /// Build the binary for a single complex cell with HAS_EMPTY_VALUE set and
    /// the given `path_bytes` as the cell path.  The timestamp field is encoded
    /// as VInt(0) (ZigZag ⇒ 0x00, one byte).
    ///
    /// Wire format (Cassandra 5.0 complex-cell layout):
    ///   [flags:u8] [timestamp:VInt] [path_len:VUInt] [path:bytes]
    pub(crate) fn build_set_cell_bytes(path: &[u8]) -> Vec<u8> {
        // flags = 0x04 (HAS_EMPTY_VALUE); use_row_timestamp bit (0x08) NOT set,
        // so an explicit timestamp follows.
        let flags: u8 = 0x04;
        // VInt(0) in ZigZag = 0x00 (single byte).
        let ts_byte: u8 = 0x00;
        // path_len as VUInt (single-byte form for small lengths).
        let path_len = path.len() as u8;
        assert!(path_len < 0x80, "helper only supports path lengths < 128");

        let mut buf = vec![flags, ts_byte, path_len];
        buf.extend_from_slice(path);
        buf
    }

    /// Build the binary for a single element-level tombstone cell of a set
    /// (Issue #493).  The element identity lives in the cell PATH, the cell has
    /// IS_DELETED (0x01) set and no value.
    ///
    /// Wire format (Cassandra 5.0 complex-cell layout), matching the read order
    /// in `parse_complex_cell_value`:
    ///   [flags:u8] [timestamp:VInt] [localDeletionTime:VUInt] [path_len:VUInt] [path:bytes]
    ///
    /// - `flags = 0x01` (IS_DELETED). use_row_timestamp (0x08) is NOT set, so an
    ///   explicit timestamp follows; use_row_ttl (0x10) is NOT set and IS_DELETED
    ///   is set, so a localDeletionTime VUInt follows. is_expiring (0x02) is NOT
    ///   set, so no TTL field follows. No value follows (cell is deleted).
    pub(crate) fn build_set_tombstone_cell_bytes(path: &[u8]) -> Vec<u8> {
        let flags: u8 = 0x01; // IS_DELETED
        let ts_byte: u8 = 0x00; // VInt(0) (ZigZag, single byte)
        let local_deletion_time: u8 = 0x01; // VUInt(1), single byte
        let path_len = path.len() as u8;
        assert!(path_len < 0x80, "helper only supports path lengths < 128");

        let mut buf = vec![flags, ts_byte, local_deletion_time, path_len];
        buf.extend_from_slice(path);
        buf
    }

    /// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value ("1"/"true").
    /// In strict mode a missing core fixture (or an unset datasets root) is a hard
    /// failure; otherwise it is a clean skip. Mirrors the sibling parity tests.
    pub(crate) fn require_fixtures_strict() -> bool {
        std::env::var("CQLITE_REQUIRE_FIXTURES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Resolve the core `test_basic/simple_table-*/*-Data.db` fixture used by the
    /// #1080 regression tests.
    ///
    /// Returns `Some(path)` when the fixture is present. Returns `None` (clean
    /// skip) when the test data is unavailable in non-strict mode — either
    /// `CQLITE_DATASETS_ROOT` is unset (local dev without data) or the bundle is
    /// absent (lanes such as Core Validation / Minimal Build set the datasets
    /// root without shipping `test_basic`).
    ///
    /// When `CQLITE_REQUIRE_FIXTURES=1` (full-dataset CI) the same conditions are
    /// a hard failure — never a silent green (issue #1094; matches the project's
    /// present-but-broken doctrine, roborev job 1359). Crucially, strict mode is
    /// also enforced when the datasets root itself is missing, so a misconfigured
    /// gate cannot false-pass.
    pub(crate) fn core_simple_table_data_file() -> Option<std::path::PathBuf> {
        let strict = require_fixtures_strict();
        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(r) => r,
            Err(_) => {
                if strict {
                    panic!(
                        "CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is unset — \
                         cannot locate the core fixture test_basic/simple_table; refusing \
                         to silently skip"
                    );
                }
                return None;
            }
        };
        let keyspace_dir = std::path::PathBuf::from(&datasets_root)
            .join("sstables")
            .join("test_basic");
        if let Ok(entries) = std::fs::read_dir(&keyspace_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.starts_with("simple_table-") {
                    if let Ok(inner) = std::fs::read_dir(&path) {
                        if let Some(df) = inner.flatten().find(|e| {
                            e.file_name()
                                .to_str()
                                .map(|s| s.ends_with("-Data.db"))
                                .unwrap_or(false)
                        }) {
                            return Some(df.path());
                        }
                    }
                }
            }
        }
        if strict {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the core fixture \
                 test_basic/simple_table-*/*-Data.db is missing under \
                 CQLITE_DATASETS_ROOT ({datasets_root}) — refusing to silently skip"
            );
        }
        eprintln!(
            "[SKIP] core fixture test_basic/simple_table-*/*-Data.db absent under \
             CQLITE_DATASETS_ROOT ({datasets_root}); set CQLITE_REQUIRE_FIXTURES=1 to enforce"
        );
        None
    }
}

// -----------------------------------------------------------------------------
// Campsite split (epic #1116, issue #3723): this `#[cfg(test)]`-only cell-header
// walk used to sit at the end of the ~920-line production `row_data.rs`. It is
// test-only support code (the Issue #623 S1 audit tests are its only callers),
// so it belongs here; moving it keeps `row_data.rs` from growing when the
// #3723 fatal-error propagation was added to the complex-column loop.
// -----------------------------------------------------------------------------
impl super::V5CompressedLegacyParser {
    /// Test-only helper that parses the cell header (flags + conditional temporal
    /// metadata) and returns the offset at which the value bytes begin.
    ///
    /// This mirrors the logic in `parse_cell_value_schema_order` for the conditional
    /// sections (Steps 1-3), but stops before the value parse.  It is used by the
    /// S1 audit verification tests (Issue #623) to confirm that:
    ///   - USE_ROW_TIMESTAMP (0x08) causes the timestamp VInt to be ABSENT
    ///   - USE_ROW_TTL (0x10) without IS_EXPIRING causes LDT/TTL to be ABSENT
    ///
    /// Returns `(flags, value_start_offset)`.
    pub(super) fn parse_cell_header_end_offset(
        &self,
        data: &[u8],
        start_offset: usize,
    ) -> Result<(u8, usize)> {
        const CELL_IS_DELETED: u8 = 0x01;
        const CELL_IS_EXPIRING: u8 = 0x02;
        const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
        const CELL_USE_ROW_TTL: u8 = 0x10;

        if start_offset >= data.len() {
            return Err(Error::corruption(
                "cell_header_end_offset: no flags byte".to_string(),
            ));
        }
        let flags = data[start_offset];
        let mut offset = start_offset + 1;

        let is_deleted = (flags & CELL_IS_DELETED) != 0;
        let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
        let use_row_timestamp = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
        let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;

        // Step 1: skip timestamp VInt if not using row timestamp.
        // Skip-only: byte advancement is identical for vint/vuint, but use the
        // UNSIGNED variant to match the writer encoding (roborev #863).
        if !use_row_timestamp {
            let (remaining, _ts_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse timestamp VInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }
        // Step 2: skip LDT VUInt if not using row TTL and (deleted or expiring)
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, _ldt_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse LDT VUInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }
        // Step 3: skip TTL VUInt if not using row TTL and expiring
        if !use_row_ttl && is_expiring {
            let (remaining, _ttl_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse TTL VUInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }

        Ok((flags, offset))
    }
}
