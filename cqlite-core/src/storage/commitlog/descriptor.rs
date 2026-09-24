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
    /// - [`Error::CorruptCommitLogFrame`] on a short/malformed header, invalid
    ///   UTF-8 or malformed/non-object parameters JSON, an invalid
    ///   `compressionClass` type, or a CRC mismatch.
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

        let params_json = std::str::from_utf8(params_bytes).map_err(|error| {
            Error::CorruptCommitLogFrame(format!(
                "descriptor parameters are not valid UTF-8: {error}"
            ))
        })?;
        let (compression_class, encrypted) = parse_params(params_json)?;

        // Keep the gate's version (validated) as the authoritative value.
        Ok(Self {
            version: gates.version,
            id,
            params_json: params_json.to_owned(),
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
/// parse it as JSON (never sniff the payload). The JSON must be an object; an
/// absent or explicit-null compression class means compression is unset, as in
/// Cassandra's uncompressed descriptors.
///
/// The key names are the ones Cassandra 5.0.2 actually writes in
/// `CommitLogDescriptor.constructParametersString` /
/// `EncryptionContext.toHeaderParameters`:
/// - `compressionClass` (`COMPRESSION_CLASS_KEY`) — a plain STRING value (the
///   compressor class name), NOT a nested `{"class": ...}` object.
///   (`compressionParameters` also exists but is informational and unneeded
///   for unsupported-payload detection.)
/// - `encCipher` / `encKeyAlias` / `encIV` — the encryption-context keys; there
///   is no `encryptionContext`/`encryption` key on the wire.
fn parse_params(json: &str) -> Result<(Option<String>, bool)> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|error| {
        Error::CorruptCommitLogFrame(format!(
            "invalid CommitLog descriptor parameters JSON: {error}"
        ))
    })?;
    let obj = value.as_object().ok_or_else(|| {
        Error::CorruptCommitLogFrame(
            "CommitLog descriptor parameters JSON must be an object".to_string(),
        )
    })?;
    // `compressionClass` is a plain string. Some configurations write it as an
    // explicit JSON `null` (key present, value null) when compression is unset,
    // rather than omitting the key; a present-but-null value must parse as "no
    // compression" (None), same as an absent key — never fail-closed on a valid
    // uncompressed segment. An empty string is likewise treated as no
    // compression. Only a present, non-null, non-empty string names a
    // compressor the reader must refuse.
    let compression_class = match obj.get("compressionClass") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(class)) if !class.is_empty() => Some(class.clone()),
        Some(serde_json::Value::String(_)) => None,
        Some(_) => {
            return Err(Error::CorruptCommitLogFrame(
                "CommitLog descriptor compressionClass must be a string or null".to_string(),
            ));
        }
    };
    // Encryption is declared by any of the real encryption-context keys being
    // present with a non-null value; unsupported values are conservatively
    // refused as encrypted rather than interpreted as a plain payload.
    // Same present-but-null hazard as
    // compression: `{"encCipher": null}` is NOT encrypted.
    let encrypted = ["encCipher", "encKeyAlias", "encIV"]
        .iter()
        .any(|k| obj.get(*k).is_some_and(|v| !v.is_null()));
    Ok((compression_class, encrypted))
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
pub(crate) mod tests {
    use super::*;

    /// Build a descriptor header the same way Cassandra's `writeHeader` does,
    /// so the parser's CRC and field offsets are exercised in isolation. This
    /// is a *unit* fixture for offset math only; the authoritative correctness
    /// oracle is the real Cassandra-produced segment (see the integration test).
    ///
    /// `pub(crate)` so the [`super::super::reader`] public-surface tests can
    /// build a Cassandra-shaped descriptor header without duplicating the CRC.
    pub(crate) fn build_header(version: i32, id: i64, params: &str) -> Vec<u8> {
        build_header_raw(version, id, params.as_bytes())
    }

    fn build_header_raw(version: i32, id: i64, params_bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&version.to_be_bytes());
        out.extend_from_slice(&id.to_be_bytes());
        let params_len = u16::try_from(params_bytes.len()).expect("test parameters fit header");
        out.extend_from_slice(&params_len.to_be_bytes());
        out.extend_from_slice(params_bytes);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&version.to_be_bytes());
        let id_u = id as u64;
        hasher.update(&((id_u & 0xFFFF_FFFF) as u32).to_be_bytes());
        hasher.update(&((id_u >> 32) as u32).to_be_bytes());
        hasher.update(&u32::from(params_len).to_be_bytes());
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
    fn rejects_invalid_utf8_in_descriptor_parameters() {
        let bytes = build_header_raw(7, 42, &[0xff]);
        assert!(matches!(
            CommitLogDescriptor::parse(&bytes),
            Err(Error::CorruptCommitLogFrame(_))
        ));
    }

    #[test]
    fn rejects_malformed_non_object_and_wrongly_typed_parameters() {
        for params in [
            "not-json",
            "[]",
            "null",
            r#"{"compressionClass":{}}"#,
            r#"{"compressionClass":[]}"#,
            r#"{"compressionClass":false}"#,
            r#"{"compressionClass":7}"#,
        ] {
            let bytes = build_header(7, 42, params);
            assert!(
                matches!(
                    CommitLogDescriptor::parse(&bytes),
                    Err(Error::CorruptCommitLogFrame(_))
                ),
                "expected malformed descriptor params to fail: {params}"
            );
        }
    }

    #[test]
    fn missing_and_null_compression_metadata_are_valid_uncompressed_controls() {
        for params in ["{}", r#"{"compressionClass":null}"#] {
            let bytes = build_header(7, 42, params);
            let desc = CommitLogDescriptor::parse(&bytes).expect("valid unset metadata");
            assert_eq!(desc.compression_class, None);
            assert!(!desc.is_unsupported_payload());
        }
    }

    #[test]
    fn detects_compression_class_from_params() {
        // Real Cassandra 5.0.2 shape: `compressionClass` is a plain string
        // (COMPRESSION_CLASS_KEY), not a nested {"class": ...} object.
        let params = r#"{"compressionClass":"LZ4Compressor","compressionParameters":{}}"#;
        let bytes = build_header(7, 7, params);
        let desc = CommitLogDescriptor::parse(&bytes).expect("parse");
        assert_eq!(desc.compression_class.as_deref(), Some("LZ4Compressor"));
        assert!(desc.is_unsupported_payload());
    }

    #[test]
    fn compression_null_parses_as_uncompressed_not_unknown() {
        // Some configurations write `"compressionClass": null` (key present,
        // explicitly null) rather than omitting the key when compression is
        // unset. This must NOT fail closed — regression for the present-but-null
        // safety posture, now applied to the real key (PR #2797 blocker).
        let params = r#"{"compressionClass":null}"#;
        let bytes = build_header(7, 9, params);
        let desc = CommitLogDescriptor::parse(&bytes).expect("parse");
        assert_eq!(desc.compression_class, None);
        assert!(!desc.is_unsupported_payload());
    }

    #[test]
    fn encryption_key_null_parses_as_unencrypted() {
        // Same present-but-null hazard as compression, for the real encryption
        // keys (encCipher/encKeyAlias/encIV) — a present-but-null encCipher is
        // NOT encrypted (PR #2797 blocker).
        let params = r#"{"compressionClass":null,"encCipher":null}"#;
        let bytes = build_header(7, 11, params);
        let desc = CommitLogDescriptor::parse(&bytes).expect("parse");
        assert_eq!(desc.compression_class, None);
        assert!(!desc.is_unsupported_payload());
    }

    #[test]
    fn detects_encryption_from_real_context_keys() {
        // Real Cassandra 5.0.2 shape: encryption is declared by encCipher /
        // encKeyAlias / encIV, not an "encryptionContext"/"encryption" key
        // (which Cassandra never writes). A present, non-null encCipher must
        // make the segment an unsupported (encrypted) payload (PR #2797 blocker).
        let params = r#"{"encCipher":"AES/CBC/PKCS5Padding","encKeyAlias":"testing:1"}"#;
        let bytes = build_header(7, 13, params);
        let desc = CommitLogDescriptor::parse(&bytes).expect("parse");
        assert!(desc.encrypted);
        assert!(desc.compression_class.is_none());
        assert!(desc.is_unsupported_payload());
    }
}
