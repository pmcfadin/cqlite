//! Cassandra CommitLog segment **descriptor** parsing (issue #2389).
//!
//! Every CommitLog segment begins with a `CommitLogDescriptor` header that
//! Cassandra writes via `CommitLogDescriptor.writeHeader` (Cassandra 5.0
//! `org.apache.cassandra.db.commitlog.CommitLogDescriptor`). The on-disk layout,
//! all big-endian, is:
//!
//! ```text
//! int32   version          descriptor/messaging version (VERSION_40 = 7 for 5.0)
//! int64   id               segment id (wall-clock-derived at segment creation)
//! int16   paramsLen        length of the UTF-8 parameters JSON
//! bytes   params           UTF-8 JSON: {} when uncompressed/unencrypted
//! int32   crc              CRC32 (java.util.zip.CRC32 / IEEE) over the header
//! ```
//!
//! The CRC covers, in order (each int fed big-endian, matching Cassandra's
//! `FBUtilities.updateChecksumInt`): `version`, the low 32 bits of `id`, the
//! high 32 bits of `id`, `paramsLen` as a 32-bit int, then the raw params bytes.
//!
//! Version and format are read **authoritatively from these header bytes**,
//! never inferred from the filename or file size (no-heuristics, issue #28).

use crate::{Error, Result};

/// The single Cassandra 5.0-era CommitLog descriptor version CQLite supports.
///
/// `CommitLogDescriptor.VERSION_40 == 7`; Cassandra 4.0 and 5.0 both write it as
/// `current_version`. Mirrors the exact-allowlist posture of
/// [`crate::storage::sstable::version_gate::BigVersionGates`].
pub const SUPPORTED_COMMITLOG_VERSION: i32 = 7;

/// Version gate for a Cassandra CommitLog segment.
///
/// Constructed only from the descriptor's authoritative `version` field. Rejects
/// any version outside the supported Cassandra 5.0-era range via a typed
/// [`Error::UnsupportedCommitLogVersion`], the same pattern as
/// `BigVersionGates`/`BtiVersionGates` for SSTables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitLogVersionGates {
    /// The descriptor version this gate set was computed from.
    pub version: i32,
}

impl CommitLogVersionGates {
    /// Validate a CommitLog descriptor version.
    ///
    /// # Errors
    /// Returns [`Error::UnsupportedCommitLogVersion`] for any version other than
    /// [`SUPPORTED_COMMITLOG_VERSION`].
    pub fn from_version(version: i32) -> Result<Self> {
        if version != SUPPORTED_COMMITLOG_VERSION {
            return Err(Error::UnsupportedCommitLogVersion {
                version,
                floor: SUPPORTED_COMMITLOG_VERSION,
                ceiling: SUPPORTED_COMMITLOG_VERSION,
            });
        }
        Ok(Self { version })
    }
}

/// Parsed CommitLog segment descriptor header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLogDescriptor {
    /// Descriptor/messaging version (authoritative, from the header).
    pub version: i32,
    /// Segment id.
    pub id: i64,
    /// Raw parameters JSON string (`{}` when uncompressed/unencrypted).
    pub params_json: String,
    /// Compressor class name if the segment declares commitlog compression.
    ///
    /// `Some(_)` means the mutation stream is compressed — CQLite v1 fails
    /// closed on these (design decision D3); it never decodes compressed bytes
    /// as if uncompressed.
    pub compression_class: Option<String>,
    /// Whether the descriptor declares an encryption context (also unsupported).
    pub encrypted: bool,
    /// Number of header bytes consumed — the offset at which the segment body
    /// (first sync marker) begins.
    pub header_len: usize,
}

impl CommitLogDescriptor {
    /// Parse a descriptor from the start of a segment's bytes.
    ///
    /// Validates the header CRC and the version gate before returning. Does not
    /// touch the mutation stream.
    ///
    /// # Errors
    /// - [`Error::CorruptCommitLogFrame`] on a short/malformed header or a CRC
    ///   mismatch.
    /// - [`Error::UnsupportedCommitLogVersion`] for an out-of-range version.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        // version(4) + id(8) + paramsLen(2) + crc(4) = 18 byte minimum.
        if bytes.len() < 18 {
            return Err(Error::CorruptCommitLogFrame(format!(
                "segment too short for a descriptor header: {} bytes",
                bytes.len()
            )));
        }

        let version = read_i32_be(bytes, 0);
        // Gate the version FIRST (authoritative) — but only after confirming the
        // header is structurally readable, so we never parse a bogus version off
        // a truncated file as if meaningful.
        let gates = CommitLogVersionGates::from_version(version)?;

        let id = read_i64_be(bytes, 4);
        let params_len = read_u16_be(bytes, 12) as usize;

        let params_start = 14;
        let params_end = params_start + params_len;
        let crc_end = params_end + 4;
        if bytes.len() < crc_end {
            return Err(Error::CorruptCommitLogFrame(format!(
                "descriptor header claims {} param bytes but segment is only {} bytes",
                params_len,
                bytes.len()
            )));
        }

        let params_bytes = &bytes[params_start..params_end];
        let stored_crc = read_u32_be(bytes, params_end);

        // CRC over version, id-low, id-high, paramsLen(int32), params bytes.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&version.to_be_bytes());
        let id_u = id as u64;
        hasher.update(&((id_u & 0xFFFF_FFFF) as u32).to_be_bytes());
        hasher.update(&((id_u >> 32) as u32).to_be_bytes());
        hasher.update(&(params_len as u32).to_be_bytes());
        hasher.update(params_bytes);
        let computed_crc = hasher.finalize();

        if computed_crc != stored_crc {
            return Err(Error::CorruptCommitLogFrame(format!(
                "descriptor CRC mismatch: stored={stored_crc:#010x} computed={computed_crc:#010x}"
            )));
        }

        let params_json = String::from_utf8_lossy(params_bytes).into_owned();
        let (compression_class, encrypted) = parse_params(&params_json);

        // Keep the gate's version (validated) as the authoritative value.
        Ok(Self {
            version: gates.version,
            id,
            params_json,
            compression_class,
            encrypted,
            header_len: crc_end,
        })
    }

    /// Whether the segment declares an unsupported (compressed or encrypted)
    /// payload. When true, [`crate::storage::commitlog::CommitLogReader`] fails
    /// closed rather than misdecoding the stream.
    pub fn is_unsupported_payload(&self) -> bool {
        self.compression_class.is_some() || self.encrypted
    }
}

/// Extract the compressor class and encryption flag from the params JSON.
///
/// The params object is Cassandra's own authoritative descriptor metadata; we
/// parse it as JSON (never sniff the payload). An unparseable/empty params
/// string is treated as uncompressed+unencrypted (the `{}` baseline).
fn parse_params(json: &str) -> (Option<String>, bool) {
    let value: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return (None, false),
    };
    let obj = match value.as_object() {
        Some(o) => o,
        None => return (None, false),
    };
    // Cassandra keys: "compression" -> {"class": ..., "options": {...}}.
    let compression_class = obj.get("compression").and_then(|c| {
        c.get("class")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| Some("<unknown>".to_string()))
    });
    let encrypted = obj.contains_key("encryptionContext") || obj.contains_key("encryption");
    (compression_class, encrypted)
}

#[inline]
fn read_u16_be(b: &[u8], off: usize) -> u16 {
    u16::from_be_bytes([b[off], b[off + 1]])
}
#[inline]
fn read_i32_be(b: &[u8], off: usize) -> i32 {
    i32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
#[inline]
fn read_u32_be(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
#[inline]
fn read_i64_be(b: &[u8], off: usize) -> i64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[off..off + 8]);
    i64::from_be_bytes(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a descriptor header the same way Cassandra's `writeHeader` does,
    /// so the parser's CRC and field offsets are exercised in isolation. This
    /// is a *unit* fixture for offset math only; the authoritative correctness
    /// oracle is the real Cassandra-produced segment (see the integration test).
    fn build_header(version: i32, id: i64, params: &str) -> Vec<u8> {
        let params_bytes = params.as_bytes();
        let mut out = Vec::new();
        out.extend_from_slice(&version.to_be_bytes());
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(&(params_bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(params_bytes);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&version.to_be_bytes());
        let id_u = id as u64;
        hasher.update(&((id_u & 0xFFFF_FFFF) as u32).to_be_bytes());
        hasher.update(&((id_u >> 32) as u32).to_be_bytes());
        hasher.update(&(params_bytes.len() as u32).to_be_bytes());
        hasher.update(params_bytes);
        out.extend_from_slice(&hasher.finalize().to_be_bytes());
        out
    }

    #[test]
    fn version_gate_accepts_only_v7() {
        assert!(CommitLogVersionGates::from_version(7).is_ok());
        for v in [-1, 0, 5, 6, 8, 100] {
            assert!(matches!(
                CommitLogVersionGates::from_version(v),
                Err(Error::UnsupportedCommitLogVersion { .. })
            ));
        }
    }

    #[test]
    fn parses_well_formed_uncompressed_header() {
        let id = 1_689_012_345_678_i64;
        let bytes = build_header(7, id, "{}");
        let desc = CommitLogDescriptor::parse(&bytes).expect("parse");
        assert_eq!(desc.version, 7);
        assert_eq!(desc.id, id);
        assert_eq!(desc.header_len, bytes.len());
        assert!(!desc.is_unsupported_payload());
        assert!(desc.compression_class.is_none());
    }

    #[test]
    fn rejects_unsupported_version_before_body() {
        let bytes = build_header(6, 42, "{}");
        assert!(matches!(
            CommitLogDescriptor::parse(&bytes),
            Err(Error::UnsupportedCommitLogVersion { version: 6, .. })
        ));
    }

    #[test]
    fn rejects_crc_mismatch() {
        let mut bytes = build_header(7, 42, "{}");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(matches!(
            CommitLogDescriptor::parse(&bytes),
            Err(Error::CorruptCommitLogFrame(_))
        ));
    }

    #[test]
    fn rejects_short_header() {
        assert!(matches!(
            CommitLogDescriptor::parse(&[0u8; 4]),
            Err(Error::CorruptCommitLogFrame(_))
        ));
    }

    #[test]
    fn detects_compression_class_from_params() {
        let params = r#"{"compression":{"class":"LZ4Compressor","options":{}}}"#;
        let bytes = build_header(7, 7, params);
        let desc = CommitLogDescriptor::parse(&bytes).expect("parse");
        assert_eq!(desc.compression_class.as_deref(), Some("LZ4Compressor"));
        assert!(desc.is_unsupported_payload());
    }
}
