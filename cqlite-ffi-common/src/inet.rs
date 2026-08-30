//! The single decision of what a CQL `inet` byte string is (issue #1452).
//!
//! A CQL `inet` value is authoritatively 4 bytes (IPv4) or 16 bytes (IPv6).
//! Any other length is corrupt data, and per the no-heuristics mandate (issue
//! #28) the only outcomes are IPv4, IPv6 and a typed error — never a raw-bytes
//! passthrough and never a hex fallback.
//!
//! # Why two functions
//!
//! The bindings genuinely need different halves of the same decision, and the
//! shared part is the **length dispatch plus the error**, not the formatting:
//!
//! * Node renders a string, so it calls [`inet_bytes_to_string`].
//! * Python builds `ipaddress.IPv4Address` / `IPv6Address` **from the packed
//!   bytes** and never formats an address itself, so it calls [`inet_kind`] to
//!   pick the class.
//!
//! Both render a malformed length through [`InetError`]'s `Display`, which is
//! THE single spelling of that message in the repository. Issue #1453 had
//! aligned the two bindings by hand-copying the message text into both files —
//! the same "one fact written twice" shape this crate removes.

use std::net::{Ipv4Addr, Ipv6Addr};

/// Which address family a well-formed CQL `inet` payload denotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InetKind {
    /// A 4-byte payload: IPv4.
    V4,
    /// A 16-byte payload: IPv6.
    V6,
}

/// A CQL `inet` payload whose length is neither 4 nor 16 — corrupt data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InetError {
    /// The offending payload length, in bytes.
    pub len: usize,
}

impl std::fmt::Display for InetError {
    /// THE single spelling of the malformed-length message, in both bindings.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Invalid inet address length: {} (expected 4 or 16)",
            self.len
        )
    }
}

impl std::error::Error for InetError {}

/// Classify a CQL `inet` payload by length, or report it as malformed.
pub fn inet_kind(bytes: &[u8]) -> Result<InetKind, InetError> {
    match bytes.len() {
        4 => Ok(InetKind::V4),
        16 => Ok(InetKind::V6),
        len => Err(InetError { len }),
    }
}

/// Render a CQL `inet` payload as an IP-address string.
///
/// IPv6 uses the compressed textual form (`2001:db8::1`), matching both
/// `std::net::Ipv6Addr`'s `Display` and Python's `ipaddress.IPv6Address.__str__`.
pub fn inet_bytes_to_string(bytes: &[u8]) -> Result<String, InetError> {
    let malformed = || InetError { len: bytes.len() };
    match inet_kind(bytes)? {
        // The `try_into` cannot fail — `inet_kind` just proved the length — but
        // it is expressed as a typed error rather than an `unwrap`, so library
        // code carries no panicking call at all.
        InetKind::V4 => {
            let octets: [u8; 4] = bytes.try_into().map_err(|_| malformed())?;
            Ok(Ipv4Addr::from(octets).to_string())
        }
        InetKind::V6 => {
            let octets: [u8; 16] = bytes.try_into().map_err(|_| malformed())?;
            Ok(Ipv6Addr::from(octets).to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_renders_dotted_quad() {
        assert_eq!(inet_kind(&[192, 168, 1, 1]), Ok(InetKind::V4));
        assert_eq!(
            inet_bytes_to_string(&[192, 168, 1, 1]),
            Ok("192.168.1.1".to_string())
        );
        assert_eq!(
            inet_bytes_to_string(&[0, 0, 0, 0]),
            Ok("0.0.0.0".to_string())
        );
        assert_eq!(
            inet_bytes_to_string(&[255, 255, 255, 255]),
            Ok("255.255.255.255".to_string())
        );
    }

    /// Moved from `bindings/python/src/value.rs` (issue #1452): the two inet unit
    /// tests there exercised a `#[cfg(test)]`-only THIRD formatter whose hex
    /// fallback contradicted production behaviour. Here they exercise the
    /// production path.
    #[test]
    fn ipv6_renders_compressed_form() {
        let raw = [
            0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70,
            0x73, 0x34,
        ];
        assert_eq!(inet_kind(&raw), Ok(InetKind::V6));
        assert_eq!(
            inet_bytes_to_string(&raw),
            Ok("2001:db8:85a3::8a2e:370:7334".to_string())
        );
        // Loopback and all-zeros are the two fully-compressible shapes.
        let mut loopback = [0u8; 16];
        loopback[15] = 1;
        assert_eq!(inet_bytes_to_string(&loopback), Ok("::1".to_string()));
        assert_eq!(inet_bytes_to_string(&[0u8; 16]), Ok("::".to_string()));
    }

    #[test]
    fn malformed_lengths_are_typed_errors_never_a_passthrough() {
        for bad_len in [0usize, 1, 2, 3, 5, 6, 8, 15, 17, 32, 64] {
            let raw = vec![0u8; bad_len];
            assert_eq!(inet_kind(&raw), Err(InetError { len: bad_len }));
            assert_eq!(
                inet_bytes_to_string(&raw),
                Err(InetError { len: bad_len }),
                "length {bad_len} must be a typed error"
            );
        }
    }

    #[test]
    fn malformed_message_has_one_spelling() {
        assert_eq!(
            InetError { len: 5 }.to_string(),
            "Invalid inet address length: 5 (expected 4 or 16)"
        );
    }
}
