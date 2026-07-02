//! Unified SSTable header parsing specification and implementation
//!
//! This module provides a spec-driven approach to parsing headers across all SSTable components
//! (Data.db, Index.db, Summary.db) based on the Cassandra SSTable format specification.
//!
//! ## Design Goals
//!
//! 1. **Specification-driven**: All parsing is based on the documented SSTable format specification
//! 2. **Unified interface**: Single header parsing system shared across all components
//! 3. **Version awareness**: Proper handling of different Cassandra versions and formats
//! 4. **Robust validation**: Comprehensive bounds checking and field validation
//! 5. **Memory efficient**: Zero-copy parsing where possible
//! 6. **Testable**: Clear separation between format specification and implementation

use crate::{
    error::{Error, Result},
    parser::{
        header::{parse_magic_and_version, CassandraVersion},
        vint::{parse_vint, parse_vint_length_signed},
    },
};
use nom::{
    bytes::complete::take,
    number::complete::{be_u16, be_u32, be_u64, le_u32},
    IResult,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// SSTable component type for format-specific parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SSTableComponentType {
    /// Data.db - main partition data
    Data,
    /// Index.db - partition index
    Index,
    /// Summary.db - sampled partition keys for range queries
    Summary,
    /// Statistics.db - SSTable metadata and statistics
    Statistics,
    /// CompressionInfo.db - compression parameters
    CompressionInfo,
    /// Filter.db - bloom filter data
    Filter,
}

/// Format specification for SSTable component headers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHeaderSpec {
    /// Component type
    pub component_type: SSTableComponentType,
    /// Whether this component uses magic number validation
    pub has_magic_number: bool,
    /// Expected magic number if applicable
    pub magic_number: Option<u32>,
    /// Minimum supported version
    pub min_version: u32,
    /// Maximum supported version
    pub max_version: u32,
    /// Header field layout specification
    pub field_layout: HeaderFieldLayout,
}

/// Header field layout specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderFieldLayout {
    /// Ordered list of fields in the header
    pub fields: Vec<HeaderField>,
    /// Total minimum header size in bytes
    pub min_size: usize,
    /// Maximum reasonable header size for validation
    pub max_size: usize,
}

/// Individual header field specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderField {
    /// Field name for debugging and validation
    pub name: String,
    /// Field data type
    pub field_type: HeaderFieldType,
    /// Whether this field is optional
    pub optional: bool,
    /// Field validation constraints
    pub validation: Option<FieldValidation>,
}

/// Header field data types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeaderFieldType {
    /// 8-bit unsigned integer
    U8,
    /// 16-bit big-endian unsigned integer
    U16BE,
    /// 32-bit big-endian unsigned integer
    U32BE,
    /// 64-bit big-endian unsigned integer
    U64BE,
    /// 32-bit little-endian unsigned integer
    U32LE,
    /// Variable-length integer (VInt)
    VInt,
    /// Variable-length string (VInt length + UTF-8 bytes)
    VString,
    /// Fixed-length byte array
    FixedBytes(usize),
    /// Variable-length byte array (VInt length + bytes)
    VBytes,
    /// Array of fields (VInt count + repeated field)
    Array(Box<HeaderFieldType>),
    /// Key-value map (VInt count + repeated key/value pairs)
    Map(Box<HeaderFieldType>, Box<HeaderFieldType>),
}

/// Field validation constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValidation {
    /// Minimum allowed value (for numeric types)
    pub min_value: Option<u64>,
    /// Maximum allowed value (for numeric types)
    pub max_value: Option<u64>,
    /// Expected specific values (enum validation)
    pub allowed_values: Option<Vec<u64>>,
    /// Maximum length (for string/bytes types)
    pub max_length: Option<usize>,
}

/// Parsed header data with type-safe access
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedHeader {
    /// Component type this header was parsed for
    pub component_type: SSTableComponentType,
    /// Detected Cassandra version
    pub cassandra_version: CassandraVersion,
    /// Format version
    pub format_version: u32,
    /// Parsed field values by name
    pub fields: HashMap<String, HeaderFieldValue>,
    /// Total header size in bytes
    pub header_size: usize,
}

/// Type-safe header field values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeaderFieldValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    VInt(i64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<HeaderFieldValue>),
    Map(HashMap<String, HeaderFieldValue>),
}

impl HeaderFieldValue {
    /// Extract u32 value with type checking
    pub fn as_u32(&self) -> Result<u32> {
        match self {
            HeaderFieldValue::U32(v) => Ok(*v),
            _ => Err(Error::corruption("Expected u32 field value".to_string())),
        }
    }

    /// Extract u64 value with type checking
    pub fn as_u64(&self) -> Result<u64> {
        match self {
            HeaderFieldValue::U64(v) => Ok(*v),
            _ => Err(Error::corruption("Expected u64 field value".to_string())),
        }
    }

    /// Extract string value with type checking
    pub fn as_string(&self) -> Result<&str> {
        match self {
            HeaderFieldValue::String(s) => Ok(s),
            _ => Err(Error::corruption("Expected string field value".to_string())),
        }
    }

    /// Extract bytes value with type checking
    pub fn as_bytes(&self) -> Result<&[u8]> {
        match self {
            HeaderFieldValue::Bytes(b) => Ok(b),
            _ => Err(Error::corruption("Expected bytes field value".to_string())),
        }
    }
}

/// Specification registry for all supported SSTable components
pub struct HeaderSpecRegistry {
    specs: HashMap<SSTableComponentType, ComponentHeaderSpec>,
}

impl Default for HeaderSpecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HeaderSpecRegistry {
    /// Create a new registry with default specifications
    pub fn new() -> Self {
        let mut registry = Self {
            specs: HashMap::new(),
        };
        registry.register_default_specs();
        registry
    }

    /// Register header specifications for all supported components
    fn register_default_specs(&mut self) {
        // Data.db header specification
        self.specs.insert(
            SSTableComponentType::Data,
            ComponentHeaderSpec {
                component_type: SSTableComponentType::Data,
                has_magic_number: true,
                magic_number: None, // Determined by parse_magic_and_version
                min_version: 1,
                max_version: 10,
                field_layout: HeaderFieldLayout {
                    fields: vec![
                        HeaderField {
                            name: "table_id".to_string(),
                            field_type: HeaderFieldType::FixedBytes(16),
                            optional: false,
                            validation: None,
                        },
                        HeaderField {
                            name: "keyspace".to_string(),
                            field_type: HeaderFieldType::VString,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: None,
                                max_value: None,
                                allowed_values: None,
                                max_length: Some(256),
                            }),
                        },
                        HeaderField {
                            name: "table_name".to_string(),
                            field_type: HeaderFieldType::VString,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: None,
                                max_value: None,
                                allowed_values: None,
                                max_length: Some(256),
                            }),
                        },
                        HeaderField {
                            name: "generation".to_string(),
                            field_type: HeaderFieldType::U64BE,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: Some(1),
                                max_value: Some(u64::MAX),
                                allowed_values: None,
                                max_length: None,
                            }),
                        },
                    ],
                    min_size: 32,
                    max_size: 1024,
                },
            },
        );

        // Index.db header specification
        self.specs.insert(
            SSTableComponentType::Index,
            ComponentHeaderSpec {
                component_type: SSTableComponentType::Index,
                has_magic_number: false, // Legacy format without magic number
                magic_number: None,
                min_version: 1,
                max_version: 10,
                field_layout: HeaderFieldLayout {
                    fields: vec![
                        HeaderField {
                            name: "version".to_string(),
                            field_type: HeaderFieldType::U32BE,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: Some(1),
                                max_value: Some(10),
                                allowed_values: None,
                                max_length: None,
                            }),
                        },
                        HeaderField {
                            name: "entry_count".to_string(),
                            field_type: HeaderFieldType::U32BE,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: Some(0),
                                max_value: Some(100_000_000),
                                allowed_values: None,
                                max_length: None,
                            }),
                        },
                        HeaderField {
                            name: "data_size".to_string(),
                            field_type: HeaderFieldType::U64BE,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: Some(0),
                                max_value: Some(1_000_000_000_000), // 1TB limit
                                allowed_values: None,
                                max_length: None,
                            }),
                        },
                        HeaderField {
                            name: "checksum".to_string(),
                            field_type: HeaderFieldType::U32BE,
                            optional: false,
                            validation: None,
                        },
                    ],
                    min_size: 16,
                    max_size: 64,
                },
            },
        );

        // Summary.db header specification
        self.specs.insert(
            SSTableComponentType::Summary,
            ComponentHeaderSpec {
                component_type: SSTableComponentType::Summary,
                has_magic_number: false, // Default to legacy format
                magic_number: None,
                min_version: 1,
                max_version: 10,
                field_layout: HeaderFieldLayout {
                    fields: vec![
                        HeaderField {
                            name: "version".to_string(),
                            field_type: HeaderFieldType::U32BE,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: Some(1),
                                max_value: Some(10),
                                allowed_values: None,
                                max_length: None,
                            }),
                        },
                        HeaderField {
                            name: "entry_count".to_string(),
                            field_type: HeaderFieldType::U32BE,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: Some(0),
                                max_value: Some(100_000_000),
                                allowed_values: None,
                                max_length: None,
                            }),
                        },
                        HeaderField {
                            name: "sampling_rate".to_string(),
                            field_type: HeaderFieldType::U32BE,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: Some(1),
                                max_value: Some(1_000_000),
                                allowed_values: None,
                                max_length: None,
                            }),
                        },
                        HeaderField {
                            name: "min_token".to_string(),
                            field_type: HeaderFieldType::U64BE,
                            optional: false,
                            validation: None,
                        },
                        HeaderField {
                            name: "max_token".to_string(),
                            field_type: HeaderFieldType::U64BE,
                            optional: false,
                            validation: None,
                        },
                        HeaderField {
                            name: "data_size".to_string(),
                            field_type: HeaderFieldType::U64BE,
                            optional: false,
                            validation: Some(FieldValidation {
                                min_value: Some(1),
                                max_value: Some(1_000_000_000),
                                allowed_values: None,
                                max_length: None,
                            }),
                        },
                        HeaderField {
                            name: "checksum".to_string(),
                            field_type: HeaderFieldType::U32BE,
                            optional: false,
                            validation: None,
                        },
                    ],
                    min_size: 32,
                    max_size: 1024,
                },
            },
        );
    }

    /// Get specification for a component type
    pub fn get_spec(&self, component_type: SSTableComponentType) -> Result<&ComponentHeaderSpec> {
        self.specs.get(&component_type).ok_or_else(|| {
            Error::unsupported_format(format!(
                "No specification for component: {:?}",
                component_type
            ))
        })
    }

    /// Parse header for specified component type
    pub fn parse_header(
        &self,
        input: &[u8],
        component_type: SSTableComponentType,
    ) -> Result<ParsedHeader> {
        let spec = self.get_spec(component_type)?;
        parse_component_header(input, spec)
    }
}

/// Parse header for a specific SSTable component according to specification
pub fn parse_component_header(input: &[u8], spec: &ComponentHeaderSpec) -> Result<ParsedHeader> {
    let original_input = input;
    let mut remaining = input;
    let mut fields = HashMap::new();

    // Validate minimum input size
    if input.len() < spec.field_layout.min_size {
        return Err(Error::corruption(format!(
            "Insufficient data for {:?} header: need {} bytes, have {}",
            spec.component_type,
            spec.field_layout.min_size,
            input.len()
        )));
    }

    // Parse magic number and version for components that support it
    let (cassandra_version, format_version) = if spec.has_magic_number {
        // Try to parse magic number if expected
        if let Some(expected_magic) = spec.magic_number {
            // For Summary.db with known magic number ("CQST")
            if remaining.len() < 4 {
                return Err(Error::corruption(
                    "Insufficient data for magic number".to_string(),
                ));
            }
            let (new_remaining, magic) = be_u32::<_, nom::error::Error<&[u8]>>(remaining)
                .map_err(|e| Error::corruption(format!("Failed to parse magic: {:?}", e)))?;
            if magic != expected_magic {
                return Err(Error::corruption(format!(
                    "Magic number mismatch: expected 0x{:08X}, got 0x{:08X}",
                    expected_magic, magic
                )));
            }
            remaining = new_remaining;

            // Parse version after magic
            let (new_remaining, version) = be_u32::<_, nom::error::Error<&[u8]>>(remaining)
                .map_err(|e| Error::corruption(format!("Failed to parse version: {:?}", e)))?;
            remaining = new_remaining;

            (CassandraVersion::Legacy, version as u16)
        } else {
            // For Data.db with dynamic magic numbers
            let (new_remaining, (version, format_ver)) = parse_magic_and_version(remaining)
                .map_err(|e| {
                    Error::corruption(format!("Failed to parse magic/version: {:?}", e))
                })?;
            remaining = new_remaining;
            (version, format_ver)
        }
    } else {
        // For legacy components without magic numbers (Index.db),
        // version will be parsed as part of the field layout
        (CassandraVersion::Legacy, 1u16) // Default version, actual version comes from fields
    };

    // Parse each field according to specification
    for field in &spec.field_layout.fields {
        let (new_remaining, value) =
            parse_header_field(remaining, &field.field_type, &field.validation).map_err(|e| {
                Error::corruption(format!("Failed to parse field '{}': {:?}", field.name, e))
            })?;

        remaining = new_remaining;
        fields.insert(field.name.clone(), value);
    }

    // Calculate total header size
    let header_size = original_input.len() - remaining.len();

    // Validate header size is reasonable
    if header_size > spec.field_layout.max_size {
        return Err(Error::corruption(format!(
            "Header size {} exceeds maximum {} for {:?}",
            header_size, spec.field_layout.max_size, spec.component_type
        )));
    }

    // For legacy components without magic numbers, update format_version from parsed version field
    let actual_format_version = if !spec.has_magic_number {
        if let Some(HeaderFieldValue::U32(version)) = fields.get("version") {
            *version as u16
        } else {
            format_version
        }
    } else {
        format_version
    };

    Ok(ParsedHeader {
        component_type: spec.component_type,
        cassandra_version,
        format_version: actual_format_version.into(),
        fields,
        header_size,
    })
}

/// Parse individual header field according to type specification
fn parse_header_field<'a>(
    input: &'a [u8],
    field_type: &HeaderFieldType,
    validation: &Option<FieldValidation>,
) -> IResult<&'a [u8], HeaderFieldValue> {
    use nom::error::{Error as NomError, ErrorKind};

    let (remaining, value) = match field_type {
        HeaderFieldType::U8 => {
            let (remaining, val) = nom::number::complete::be_u8(input)?;
            (remaining, HeaderFieldValue::U8(val))
        }
        HeaderFieldType::U16BE => {
            let (remaining, val) = be_u16(input)?;
            (remaining, HeaderFieldValue::U16(val))
        }
        HeaderFieldType::U32BE => {
            let (remaining, val) = be_u32(input)?;
            (remaining, HeaderFieldValue::U32(val))
        }
        HeaderFieldType::U64BE => {
            let (remaining, val) = be_u64(input)?;
            (remaining, HeaderFieldValue::U64(val))
        }
        HeaderFieldType::U32LE => {
            let (remaining, val) = le_u32(input)?;
            (remaining, HeaderFieldValue::U32(val))
        }
        HeaderFieldType::VInt => {
            let (remaining, val) = parse_vint(input)?;
            (remaining, HeaderFieldValue::VInt(val))
        }
        HeaderFieldType::VString => {
            // VString format: one byte length followed by UTF-8 bytes
            if input.is_empty() {
                return Err(nom::Err::Error(NomError::new(input, ErrorKind::Eof)));
            }
            let len = input[0] as usize;
            if input.len() < 1 + len {
                return Err(nom::Err::Error(NomError::new(input, ErrorKind::Eof)));
            }
            let (remaining, _) = take(1usize)(input)?; // consume length byte
            let (remaining, bytes) = take(len)(remaining)?;
            let string = String::from_utf8(bytes.to_vec())
                .map_err(|_| nom::Err::Error(NomError::new(input, ErrorKind::Verify)))?;
            (remaining, HeaderFieldValue::String(string))
        }
        HeaderFieldType::FixedBytes(size) => {
            let (remaining, bytes) = take(*size)(input)?;
            (remaining, HeaderFieldValue::Bytes(bytes.to_vec()))
        }
        HeaderFieldType::VBytes => {
            let (remaining, len) = parse_vint_length_signed(input)?;
            let (remaining, bytes) = take(len)(remaining)?;
            (remaining, HeaderFieldValue::Bytes(bytes.to_vec()))
        }
        HeaderFieldType::Array(element_type) => {
            let (remaining, count) = parse_vint_length_signed(input)?;
            let mut elements = Vec::new();
            let mut current = remaining;

            for _ in 0..count {
                let (new_current, element) = parse_header_field(current, element_type, &None)?;
                elements.push(element);
                current = new_current;
            }

            (current, HeaderFieldValue::Array(elements))
        }
        HeaderFieldType::Map(key_type, value_type) => {
            let (remaining, count) = parse_vint_length_signed(input)?;
            let mut map = HashMap::new();
            let mut current = remaining;

            for _ in 0..count {
                let (new_current, key) = parse_header_field(current, key_type, &None)?;
                let (new_current, value) = parse_header_field(new_current, value_type, &None)?;

                let key_str = match key {
                    HeaderFieldValue::String(s) => s,
                    _ => return Err(nom::Err::Error(NomError::new(input, ErrorKind::Verify))),
                };

                map.insert(key_str, value);
                current = new_current;
            }

            (current, HeaderFieldValue::Map(map))
        }
    };

    // Apply validation if specified
    if let Some(validation) = validation {
        validate_field_value(&value, validation)
            .map_err(|_| nom::Err::Error(NomError::new(input, ErrorKind::Verify)))?;
    }

    Ok((remaining, value))
}

/// Validate field value against constraints
fn validate_field_value(value: &HeaderFieldValue, validation: &FieldValidation) -> Result<()> {
    // Validate numeric ranges with proper handling of both signed and unsigned values
    if validation.min_value.is_some() || validation.max_value.is_some() {
        let num_value = match value {
            HeaderFieldValue::U8(v) => *v as u64,
            HeaderFieldValue::U16(v) => *v as u64,
            HeaderFieldValue::U32(v) => *v as u64,
            HeaderFieldValue::U64(v) => *v,
            HeaderFieldValue::VInt(v) => {
                // For VInt, handle negative values carefully
                if *v < 0 {
                    // Check only minimum for negative values
                    if let Some(min) = validation.min_value {
                        if *v < (min as i64) {
                            return Err(Error::corruption(format!(
                                "Field value {} below minimum {}",
                                v, min
                            )));
                        }
                    }
                    return Ok(()); // Skip max validation for negative VInt
                } else {
                    *v as u64
                }
            }
            _ => return Ok(()), // Skip validation for non-numeric types
        };

        if let Some(min) = validation.min_value {
            if num_value < min {
                return Err(Error::corruption(format!(
                    "Field value {} below minimum {}",
                    num_value, min
                )));
            }
        }

        if let Some(max) = validation.max_value {
            if num_value > max {
                return Err(Error::corruption(format!(
                    "Field value {} above maximum {}",
                    num_value, max
                )));
            }
        }
    }

    // Validate string/bytes length
    if let Some(max_len) = validation.max_length {
        let actual_len = match value {
            HeaderFieldValue::String(s) => s.len(),
            HeaderFieldValue::Bytes(b) => b.len(),
            _ => return Ok(()), // Skip validation for other types
        };

        if actual_len > max_len {
            return Err(Error::corruption(format!(
                "Field length {} exceeds maximum {}",
                actual_len, max_len
            )));
        }
    }

    // Validate allowed values
    if let Some(allowed) = &validation.allowed_values {
        let num_value = match value {
            HeaderFieldValue::U8(v) => *v as u64,
            HeaderFieldValue::U16(v) => *v as u64,
            HeaderFieldValue::U32(v) => *v as u64,
            HeaderFieldValue::U64(v) => *v,
            HeaderFieldValue::VInt(v) => (*v).unsigned_abs(),
            _ => return Ok(()), // Skip validation for non-numeric types
        };

        if !allowed.contains(&num_value) {
            return Err(Error::corruption(format!(
                "Field value {} not in allowed values: {:?}",
                num_value, allowed
            )));
        }
    }

    Ok(())
}

/// Convenience parsers for each component type
impl HeaderSpecRegistry {
    /// Parse Data.db header
    pub fn parse_data_header(&self, input: &[u8]) -> Result<ParsedHeader> {
        self.parse_header(input, SSTableComponentType::Data)
    }

    /// Parse Index.db header
    pub fn parse_index_header(&self, input: &[u8]) -> Result<ParsedHeader> {
        self.parse_header(input, SSTableComponentType::Index)
    }

    /// Parse Summary.db header with auto-detection of format
    pub fn parse_summary_header(&self, input: &[u8]) -> Result<ParsedHeader> {
        if input.len() < 4 {
            return Err(Error::corruption(
                "Insufficient data for Summary.db header".to_string(),
            ));
        }

        // Check if it starts with "CQST" magic number
        let potential_magic = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
        if potential_magic == 0x43515354 {
            // Use magic number format
            let mut magic_spec = self.get_spec(SSTableComponentType::Summary)?.clone();
            magic_spec.has_magic_number = true;
            magic_spec.magic_number = Some(0x43515354); // "CQST"
                                                        // Remove version field since it's handled by magic parsing
            magic_spec
                .field_layout
                .fields
                .retain(|f| f.name != "version");
            parse_component_header(input, &magic_spec)
        } else {
            // Use legacy format (no magic number)
            self.parse_header(input, SSTableComponentType::Summary)
        }
    }
}

/// Global header specification registry
static GLOBAL_REGISTRY: std::sync::OnceLock<HeaderSpecRegistry> = std::sync::OnceLock::new();

/// Get the global header specification registry
pub fn get_global_registry() -> &'static HeaderSpecRegistry {
    GLOBAL_REGISTRY.get_or_init(HeaderSpecRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = HeaderSpecRegistry::new();
        assert!(registry.get_spec(SSTableComponentType::Data).is_ok());
        assert!(registry.get_spec(SSTableComponentType::Index).is_ok());
        assert!(registry.get_spec(SSTableComponentType::Summary).is_ok());
    }

    #[test]
    fn test_field_validation() {
        let validation = FieldValidation {
            min_value: Some(1),
            max_value: Some(100),
            allowed_values: None,
            max_length: None,
        };

        let value = HeaderFieldValue::U32(50);
        assert!(validate_field_value(&value, &validation).is_ok());

        let value = HeaderFieldValue::U32(0);
        assert!(validate_field_value(&value, &validation).is_err());

        let value = HeaderFieldValue::U32(101);
        assert!(validate_field_value(&value, &validation).is_err());
    }

    #[test]
    fn test_string_length_validation() {
        let validation = FieldValidation {
            min_value: None,
            max_value: None,
            allowed_values: None,
            max_length: Some(5),
        };

        let value = HeaderFieldValue::String("test".to_string());
        assert!(validate_field_value(&value, &validation).is_ok());

        let value = HeaderFieldValue::String("toolong".to_string());
        assert!(validate_field_value(&value, &validation).is_err());
    }

    #[test]
    fn test_allowed_values_validation() {
        let validation = FieldValidation {
            min_value: None,
            max_value: None,
            allowed_values: Some(vec![1, 2, 3]),
            max_length: None,
        };

        let value = HeaderFieldValue::U32(2);
        assert!(validate_field_value(&value, &validation).is_ok());

        let value = HeaderFieldValue::U32(4);
        assert!(validate_field_value(&value, &validation).is_err());
    }
}
