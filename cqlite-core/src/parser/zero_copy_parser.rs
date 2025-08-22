//! Zero-copy parsing optimizations
//!
//! This module provides zero-copy parsing patterns to reduce string allocations
//! and improve performance for high-throughput scenarios.

use nom::{IResult, bytes::complete::tag, character::complete::alphanumeric1, sequence::tuple};
use std::borrow::Cow;
use std::collections::HashMap;

/// String interning cache for repeated values
pub struct StringInterner {
    cache: HashMap<String, &'static str>,
}

impl StringInterner {
    /// Create new string interner
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Intern a string to reduce allocations for repeated values
    pub fn intern(&mut self, s: &str) -> &str {
        if let Some(&interned) = self.cache.get(s) {
            interned
        } else {
            // SAFETY: This leaks memory intentionally for interning
            // In a production system, this should use a proper interning strategy
            let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
            self.cache.insert(s.to_string(), leaked);
            leaked
        }
    }
}

/// Zero-copy identifier parser
pub struct ZeroCopyIdentifier<'a> {
    name: Cow<'a, str>,
    quoted: bool,
}

impl<'a> ZeroCopyIdentifier<'a> {
    /// Create new identifier from borrowed string
    pub fn borrowed(name: &'a str) -> Self {
        Self {
            name: Cow::Borrowed(name),
            quoted: false,
        }
    }

    /// Create quoted identifier from borrowed string  
    pub fn quoted_borrowed(name: &'a str) -> Self {
        Self {
            name: Cow::Borrowed(name),
            quoted: true,
        }
    }

    /// Get the name as a string reference
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if identifier is quoted
    pub fn is_quoted(&self) -> bool {
        self.quoted
    }

    /// Convert to owned version when necessary
    pub fn into_owned(self) -> ZeroCopyIdentifier<'static> {
        ZeroCopyIdentifier {
            name: Cow::Owned(self.name.into_owned()),
            quoted: self.quoted,
        }
    }
}

/// Zero-copy CQL value parser
#[derive(Debug)]
pub enum ZeroCopyValue<'a> {
    Text(&'a str),
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    Blob(&'a [u8]),
    Null,
}

impl<'a> ZeroCopyValue<'a> {
    /// Parse text value without allocation
    pub fn parse_text(input: &'a str) -> IResult<&'a str, Self> {
        use nom::{
            bytes::complete::{tag, take_while},
            sequence::delimited,
        };

        let (remaining, text) =
            delimited(tag("'"), take_while(|c: char| c != '\''), tag("'"))(input)?;
        Ok((remaining, ZeroCopyValue::Text(text)))
    }

    /// Convert to owned value when crossing async boundaries
    pub fn into_owned(self) -> OwnedValue {
        match self {
            ZeroCopyValue::Text(s) => OwnedValue::Text(s.to_string()),
            ZeroCopyValue::Integer(i) => OwnedValue::Integer(i),
            ZeroCopyValue::BigInt(i) => OwnedValue::BigInt(i),
            ZeroCopyValue::Boolean(b) => OwnedValue::Boolean(b),
            ZeroCopyValue::Blob(b) => OwnedValue::Blob(b.to_vec()),
            ZeroCopyValue::Null => OwnedValue::Null,
        }
    }
}

/// Owned value for when zero-copy is not possible
#[derive(Debug, Clone)]
pub enum OwnedValue {
    Text(String),
    Integer(i32),
    BigInt(i64),
    Boolean(bool),
    Blob(Vec<u8>),
    Null,
}

/// Zero-copy memory buffer for parsing
pub struct ZeroCopyBuffer<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> ZeroCopyBuffer<'a> {
    /// Create new zero-copy buffer
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    /// Read slice without copying
    pub fn read_slice(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.position + len <= self.data.len() {
            let slice = &self.data[self.position..self.position + len];
            self.position += len;
            Some(slice)
        } else {
            None
        }
    }

    /// Peek at next bytes without advancing
    pub fn peek(&self, len: usize) -> Option<&'a [u8]> {
        if self.position + len <= self.data.len() {
            Some(&self.data[self.position..self.position + len])
        } else {
            None
        }
    }

    /// Get remaining bytes
    pub fn remaining(&self) -> &'a [u8] {
        &self.data[self.position..]
    }

    /// Check if we're at end
    pub fn is_empty(&self) -> bool {
        self.position >= self.data.len()
    }
}

/// Performance-optimized parser with zero-copy patterns
pub struct ZeroCopyParser {
    interner: StringInterner,
}

impl ZeroCopyParser {
    /// Create new zero-copy parser
    pub fn new() -> Self {
        Self {
            interner: StringInterner::new(),
        }
    }

    /// Parse identifier with zero-copy when possible
    pub fn parse_identifier<'a>(
        &mut self,
        input: &'a str,
    ) -> IResult<&'a str, ZeroCopyIdentifier<'a>> {
        let (remaining, name) = alphanumeric1(input)?;
        Ok((remaining, ZeroCopyIdentifier::borrowed(name)))
    }

    /// Parse string with interning for repeated values
    pub fn parse_interned_string<'a>(&mut self, input: &'a str) -> IResult<&'a str, &str> {
        let (remaining, (_, content, _)) = tuple((tag("'"), alphanumeric1, tag("'")))(input)?;
        let interned = self.interner.intern(content);
        Ok((remaining, interned))
    }

    /// Parse binary data with zero-copy
    pub fn parse_blob<'a>(&self, buffer: &mut ZeroCopyBuffer<'a>, len: usize) -> Option<&'a [u8]> {
        buffer.read_slice(len)
    }
}

impl Default for ZeroCopyParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory-efficient parsing statistics
#[derive(Debug, Default)]
pub struct ZeroCopyStats {
    pub bytes_parsed_zerocopy: usize,
    pub bytes_parsed_allocated: usize,
    pub allocations_avoided: usize,
    pub strings_interned: usize,
}

impl ZeroCopyStats {
    /// Calculate memory efficiency ratio
    pub fn efficiency_ratio(&self) -> f64 {
        if self.bytes_parsed_allocated == 0 {
            return 1.0;
        }
        self.bytes_parsed_zerocopy as f64
            / (self.bytes_parsed_zerocopy + self.bytes_parsed_allocated) as f64
    }

    /// Calculate allocation avoidance ratio
    pub fn allocation_avoidance_ratio(&self) -> f64 {
        let total_potential = self.allocations_avoided + self.bytes_parsed_allocated;
        if total_potential == 0 {
            return 1.0;
        }
        self.allocations_avoided as f64 / total_potential as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_interner() {
        let mut interner = StringInterner::new();

        let s1 = interner.intern("test");
        let s1_ptr = s1.as_ptr();
        let s2 = interner.intern("test");
        let s2_ptr = s2.as_ptr();

        // Should return same reference for same string
        assert_eq!(s1_ptr, s2_ptr);
    }

    #[test]
    fn test_zero_copy_identifier() {
        let name = "test_table";
        let id = ZeroCopyIdentifier::borrowed(name);

        assert_eq!(id.name(), name);
        assert!(!id.is_quoted());
    }

    #[test]
    fn test_zero_copy_buffer() {
        let data = b"hello world";
        let mut buffer = ZeroCopyBuffer::new(data);

        let slice1 = buffer.read_slice(5).unwrap();
        assert_eq!(slice1, b"hello");

        let slice2 = buffer.read_slice(6).unwrap();
        assert_eq!(slice2, b" world");

        assert!(buffer.is_empty());
    }

    #[test]
    fn test_zero_copy_value_parsing() {
        let input = "'test_value'";
        if let Ok((_, value)) = ZeroCopyValue::parse_text(input) {
            match value {
                ZeroCopyValue::Text(s) => assert_eq!(s, "test_value"),
                _ => panic!("Expected text value"),
            }
        } else {
            panic!("Failed to parse text value");
        }
    }

    #[test]
    fn test_zero_copy_stats() {
        let mut stats = ZeroCopyStats::default();
        stats.bytes_parsed_zerocopy = 800;
        stats.bytes_parsed_allocated = 200;
        stats.allocations_avoided = 10;

        assert_eq!(stats.efficiency_ratio(), 0.8);
        assert!(stats.allocation_avoidance_ratio() > 0.0);
    }
}
