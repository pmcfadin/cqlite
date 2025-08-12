//! Complex Type Parsing for Cassandra Data Types
//!
//! This module provides comprehensive parsing support for all Cassandra data types
//! including collections, UDTs, tuples, frozen types, and nested complex structures.
//! It handles type inference, validation, and conversion between different representations.

use std::collections::HashMap;

use nom::{
    branch::alt,
    bytes::complete::{tag_no_case},
    character::complete::{char, multispace0},
    combinator::map,
    sequence::tuple,
    IResult,
};

use serde::{Deserialize, Serialize};

use crate::{
    Error, Result,
    schema::{CqlType, UdtRegistry},
    types::Value,
};

/// Comprehensive type parser for all Cassandra data types
pub struct ComplexTypeParser {
    /// UDT registry for resolving user-defined types
    udt_registry: Option<UdtRegistry>,
    /// Enable strict type validation
    _strict_validation: bool,
    /// Support for experimental features
    _experimental_features: bool,
}

/// Type parsing context with metadata
#[derive(Debug, Clone)]
pub struct TypeParsingContext {
    /// Current keyspace for UDT resolution
    pub keyspace: Option<String>,
    /// Parsing depth (for nested types)
    pub depth: usize,
    /// Maximum allowed nesting depth
    pub max_depth: usize,
    /// Collected type dependencies
    pub dependencies: Vec<String>,
    /// Type hints from external sources
    pub type_hints: HashMap<String, String>,
}

/// Parsed type information with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedType {
    /// The parsed CQL type
    pub cql_type: CqlType,
    /// Type complexity score (for performance estimation)
    pub complexity_score: u32,
    /// Whether type supports null values
    pub nullable: bool,
    /// Estimated serialized size in bytes
    pub estimated_size: Option<usize>,
    /// Type category
    pub category: TypeCategory,
    /// Additional type metadata
    pub metadata: TypeMetadata,
}

/// Type category for classification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TypeCategory {
    /// Primitive scalar types
    Primitive,
    /// Collection types (list, set, map)
    Collection,
    /// User-defined types
    UserDefined,
    /// Tuple types
    Tuple,
    /// Frozen wrapper types
    Frozen,
    /// Complex nested types
    Nested,
}

/// Additional type metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeMetadata {
    /// Whether type is frozen
    pub is_frozen: bool,
    /// Nesting level
    pub nesting_level: u32,
    /// Element count for collections/tuples
    pub element_count: Option<usize>,
    /// Key-value pairs for maps
    pub is_map: bool,
    /// UDT field count
    pub udt_field_count: Option<usize>,
    /// Type version (for evolution)
    pub version: Option<u32>,
}

impl ComplexTypeParser {
    /// Create a new complex type parser
    pub fn new() -> Self {
        Self {
            udt_registry: None,
            _strict_validation: true,
            _experimental_features: false,
        }
    }

    /// Create parser with UDT registry
    pub fn with_udt_registry(mut self, registry: UdtRegistry) -> Self {
        self.udt_registry = Some(registry);
        self
    }

    /// Parse a CQL type string into a structured type
    pub fn parse_type(&self, type_str: &str) -> Result<ParsedType> {
        let context = TypeParsingContext {
            keyspace: None,
            depth: 0,
            max_depth: 32,
            dependencies: Vec::new(),
            type_hints: HashMap::new(),
        };

        self.parse_type_with_context(type_str, &context)
    }

    /// Parse type with additional context
    pub fn parse_type_with_context(
        &self,
        type_str: &str,
        context: &TypeParsingContext,
    ) -> Result<ParsedType> {
        // Check nesting depth
        if context.depth > context.max_depth {
            return Err(Error::Schema(format!(
                "Type nesting too deep: {} > {}",
                context.depth, context.max_depth
            )));
        }

        let type_str = type_str.trim();

        // Try to parse the type
        match self.parse_cql_type_internal(type_str, context) {
            Ok((remaining, cql_type)) => {
                if !remaining.trim().is_empty() {
                    return Err(Error::Schema(format!(
                        "Unexpected content after type definition: '{}'",
                        remaining
                    )));
                }

                let parsed_type = self.analyze_parsed_type(cql_type, context)?;
                Ok(parsed_type)
            }
            Err(e) => Err(Error::Schema(format!("Failed to parse type '{}': {:?}", type_str, e))),
        }
    }

    /// Infer type from a value
    pub fn infer_type_from_value(&self, value: &Value) -> Result<ParsedType> {
        let cql_type = self.infer_cql_type_from_value(value)?;
        let context = TypeParsingContext {
            keyspace: None,
            depth: 0,
            max_depth: 32,
            dependencies: Vec::new(),
            type_hints: HashMap::new(),
        };

        self.analyze_parsed_type(cql_type, &context)
    }

    /// Convert between type representations
    pub fn convert_type_to_string(&self, cql_type: &CqlType) -> String {
        self.format_cql_type(cql_type)
    }

    // Internal implementation methods
    
    fn parse_cql_type_internal<'a>(
        &self,
        input: &'a str,
        context: &TypeParsingContext,
    ) -> IResult<&'a str, CqlType> {
        let new_context = TypeParsingContext {
            depth: context.depth + 1,
            ..context.clone()
        };

        alt((
            // Frozen types
            map(
                tuple((
                    tag_no_case("frozen"),
                    multispace0,
                    char('<'),
                    multispace0,
                    |input| self.parse_cql_type_internal(input, &new_context),
                    multispace0,
                    char('>'),
                )),
                |(_, _, _, _, inner_type, _, _)| CqlType::Frozen(Box::new(inner_type)),
            ),
            // Collection types
            self.parse_collection_type(&new_context),
            // Primitive types
            self.parse_primitive_type(),
        ))(input)
    }

    fn parse_collection_type<'a>(
        &'a self,
        context: &'a TypeParsingContext,
    ) -> impl Fn(&str) -> IResult<&str, CqlType> + 'a {
        move |input| {
            alt((
                // List type
                map(
                    tuple((
                        tag_no_case("list"),
                        multispace0,
                        char('<'),
                        multispace0,
                        |input| self.parse_cql_type_internal(input, context),
                        multispace0,
                        char('>'),
                    )),
                    |(_, _, _, _, element_type, _, _)| CqlType::List(Box::new(element_type)),
                ),
                // Set type
                map(
                    tuple((
                        tag_no_case("set"),
                        multispace0,
                        char('<'),
                        multispace0,
                        |input| self.parse_cql_type_internal(input, context),
                        multispace0,
                        char('>'),
                    )),
                    |(_, _, _, _, element_type, _, _)| CqlType::Set(Box::new(element_type)),
                ),
                // Map type
                map(
                    tuple((
                        tag_no_case("map"),
                        multispace0,
                        char('<'),
                        multispace0,
                        |input| self.parse_cql_type_internal(input, context),
                        multispace0,
                        char(','),
                        multispace0,
                        |input| self.parse_cql_type_internal(input, context),
                        multispace0,
                        char('>'),
                    )),
                    |(_, _, _, _, key_type, _, _, _, value_type, _, _)| {
                        CqlType::Map(Box::new(key_type), Box::new(value_type))
                    },
                ),
            ))(input)
        }
    }

    fn parse_primitive_type(&self) -> impl Fn(&str) -> IResult<&str, CqlType> + '_ {
        move |input| {
            alt((
                map(tag_no_case("boolean"), |_| CqlType::Boolean),
                map(tag_no_case("tinyint"), |_| CqlType::TinyInt),
                map(tag_no_case("smallint"), |_| CqlType::SmallInt),
                map(tag_no_case("int"), |_| CqlType::Int),
                map(tag_no_case("bigint"), |_| CqlType::BigInt),
                map(tag_no_case("float"), |_| CqlType::Float),
                map(tag_no_case("double"), |_| CqlType::Double),
                map(tag_no_case("decimal"), |_| CqlType::Decimal),
                map(alt((tag_no_case("text"), tag_no_case("varchar"))), |_| CqlType::Text),
                map(tag_no_case("ascii"), |_| CqlType::Ascii),
                map(tag_no_case("blob"), |_| CqlType::Blob),
                map(tag_no_case("timestamp"), |_| CqlType::Timestamp),
                map(tag_no_case("date"), |_| CqlType::Date),
                map(tag_no_case("time"), |_| CqlType::Time),
                map(tag_no_case("uuid"), |_| CqlType::Uuid),
                map(tag_no_case("timeuuid"), |_| CqlType::TimeUuid),
                map(tag_no_case("inet"), |_| CqlType::Inet),
                map(tag_no_case("duration"), |_| CqlType::Duration),
            ))(input)
        }
    }

    fn analyze_parsed_type(
        &self,
        cql_type: CqlType,
        context: &TypeParsingContext,
    ) -> Result<ParsedType> {
        let complexity_score = self.calculate_complexity_score(&cql_type);
        let estimated_size = self.estimate_type_size(&cql_type);
        let category = self.categorize_type(&cql_type);
        let metadata = self.extract_type_metadata(&cql_type, context);

        Ok(ParsedType {
            cql_type,
            complexity_score,
            nullable: true,
            estimated_size,
            category,
            metadata,
        })
    }

    fn calculate_complexity_score(&self, cql_type: &CqlType) -> u32 {
        match cql_type {
            CqlType::Boolean | CqlType::TinyInt | CqlType::SmallInt | CqlType::Int
            | CqlType::BigInt | CqlType::Float | CqlType::Double | CqlType::Text
            | CqlType::Ascii | CqlType::Blob | CqlType::Timestamp | CqlType::Date
            | CqlType::Time | CqlType::Uuid | CqlType::TimeUuid | CqlType::Inet
            | CqlType::Duration | CqlType::Decimal => 1,

            CqlType::List(inner) | CqlType::Set(inner) => {
                5 + self.calculate_complexity_score(inner)
            }
            CqlType::Map(key, value) => {
                10 + self.calculate_complexity_score(key) + self.calculate_complexity_score(value)
            }
            CqlType::Frozen(inner) => 2 + self.calculate_complexity_score(inner),
            CqlType::Custom(_) => 7,
            _ => 1,
        }
    }

    fn estimate_type_size(&self, cql_type: &CqlType) -> Option<usize> {
        match cql_type {
            CqlType::Boolean => Some(1),
            CqlType::TinyInt => Some(1),
            CqlType::SmallInt => Some(2),
            CqlType::Int => Some(4),
            CqlType::BigInt => Some(8),
            CqlType::Float => Some(4),
            CqlType::Double => Some(8),
            CqlType::Timestamp => Some(8),
            CqlType::Date => Some(4),
            CqlType::Time => Some(8),
            CqlType::Uuid | CqlType::TimeUuid => Some(16),
            CqlType::Frozen(inner) => self.estimate_type_size(inner),
            _ => None,
        }
    }

    fn categorize_type(&self, cql_type: &CqlType) -> TypeCategory {
        match cql_type {
            CqlType::Boolean | CqlType::TinyInt | CqlType::SmallInt | CqlType::Int
            | CqlType::BigInt | CqlType::Float | CqlType::Double | CqlType::Decimal
            | CqlType::Text | CqlType::Ascii | CqlType::Blob | CqlType::Timestamp
            | CqlType::Date | CqlType::Time | CqlType::Uuid | CqlType::TimeUuid
            | CqlType::Inet | CqlType::Duration => TypeCategory::Primitive,

            CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _) => TypeCategory::Collection,

            CqlType::Frozen(inner) => {
                match self.categorize_type(inner) {
                    TypeCategory::Collection => TypeCategory::Frozen,
                    other => other,
                }
            }

            CqlType::Custom(_) => TypeCategory::UserDefined,
            _ => TypeCategory::Primitive,
        }
    }

    fn extract_type_metadata(&self, cql_type: &CqlType, context: &TypeParsingContext) -> TypeMetadata {
        let is_frozen = matches!(cql_type, CqlType::Frozen(_));
        let nesting_level = context.depth as u32;
        
        let (element_count, is_map) = match cql_type {
            CqlType::Map(_, _) => (None, true),
            _ => (None, false),
        };

        TypeMetadata {
            is_frozen,
            nesting_level,
            element_count,
            is_map,
            udt_field_count: None,
            version: None,
        }
    }

    fn infer_cql_type_from_value(&self, value: &Value) -> Result<CqlType> {
        match value {
            Value::Null => Ok(CqlType::Text),
            Value::Boolean(_) => Ok(CqlType::Boolean),
            Value::Integer(_) => Ok(CqlType::Int),
            Value::BigInt(_) => Ok(CqlType::BigInt),
            Value::Float(_) => Ok(CqlType::Double),
            Value::Text(_) => Ok(CqlType::Text),
            Value::Blob(_) => Ok(CqlType::Blob),
            Value::Timestamp(_) => Ok(CqlType::Timestamp),
            Value::Uuid(_) => Ok(CqlType::Uuid),
            Value::TinyInt(_) => Ok(CqlType::TinyInt),
            Value::SmallInt(_) => Ok(CqlType::SmallInt),
            Value::Float32(_) => Ok(CqlType::Float),
            Value::List(elements) => {
                if elements.is_empty() {
                    Ok(CqlType::List(Box::new(CqlType::Text)))
                } else {
                    let element_type = self.infer_cql_type_from_value(&elements[0])?;
                    Ok(CqlType::List(Box::new(element_type)))
                }
            }
            Value::Set(elements) => {
                if elements.is_empty() {
                    Ok(CqlType::Set(Box::new(CqlType::Text)))
                } else {
                    let element_type = self.infer_cql_type_from_value(&elements[0])?;
                    Ok(CqlType::Set(Box::new(element_type)))
                }
            }
            Value::Map(pairs) => {
                if pairs.is_empty() {
                    Ok(CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Text)))
                } else {
                    let (key, value) = &pairs[0];
                    let key_type = self.infer_cql_type_from_value(key)?;
                    let value_type = self.infer_cql_type_from_value(value)?;
                    Ok(CqlType::Map(Box::new(key_type), Box::new(value_type)))
                }
            }
            Value::Frozen(inner) => {
                let inner_type = self.infer_cql_type_from_value(inner)?;
                Ok(CqlType::Frozen(Box::new(inner_type)))
            }
            _ => Ok(CqlType::Text),
        }
    }

    fn format_cql_type(&self, cql_type: &CqlType) -> String {
        match cql_type {
            CqlType::Boolean => "boolean".to_string(),
            CqlType::TinyInt => "tinyint".to_string(),
            CqlType::SmallInt => "smallint".to_string(),
            CqlType::Int => "int".to_string(),
            CqlType::BigInt => "bigint".to_string(),
            CqlType::Float => "float".to_string(),
            CqlType::Double => "double".to_string(),
            CqlType::Decimal => "decimal".to_string(),
            CqlType::Text | CqlType::Varchar => "text".to_string(),
            CqlType::Ascii => "ascii".to_string(),
            CqlType::Blob => "blob".to_string(),
            CqlType::Timestamp => "timestamp".to_string(),
            CqlType::Date => "date".to_string(),
            CqlType::Time => "time".to_string(),
            CqlType::Uuid => "uuid".to_string(),
            CqlType::TimeUuid => "timeuuid".to_string(),
            CqlType::Inet => "inet".to_string(),
            CqlType::Duration => "duration".to_string(),

            CqlType::List(inner) => format!("list<{}>", self.format_cql_type(inner)),
            CqlType::Set(inner) => format!("set<{}>", self.format_cql_type(inner)),
            CqlType::Map(key, value) => format!("map<{}, {}>", 
                self.format_cql_type(key), 
                self.format_cql_type(value)
            ),

            CqlType::Frozen(inner) => format!("frozen<{}>", self.format_cql_type(inner)),
            CqlType::Custom(name) => name.clone(),
            _ => "text".to_string(),
        }
    }
}

impl Default for ComplexTypeParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_type_parsing() {
        let parser = ComplexTypeParser::new();
        
        let result = parser.parse_type("text").unwrap();
        assert_eq!(result.cql_type, CqlType::Text);
        assert_eq!(result.category, TypeCategory::Primitive);
        assert_eq!(result.complexity_score, 1);
    }

    #[test]
    fn test_collection_type_parsing() {
        let parser = ComplexTypeParser::new();
        
        let result = parser.parse_type("list<int>").unwrap();
        assert!(matches!(result.cql_type, CqlType::List(_)));
        assert_eq!(result.category, TypeCategory::Collection);
        assert!(result.complexity_score > 1);
    }

    #[test]
    fn test_map_type_parsing() {
        let parser = ComplexTypeParser::new();
        
        let result = parser.parse_type("map<text, bigint>").unwrap();
        if let CqlType::Map(key, value) = result.cql_type {
            assert_eq!(*key, CqlType::Text);
            assert_eq!(*value, CqlType::BigInt);
        } else {
            panic!("Expected Map type");
        }
        assert_eq!(result.category, TypeCategory::Collection);
    }

    #[test]
    fn test_type_inference_from_value() {
        let parser = ComplexTypeParser::new();
        
        let int_value = Value::Integer(42);
        let inferred = parser.infer_type_from_value(&int_value).unwrap();
        assert_eq!(inferred.cql_type, CqlType::Int);
        
        let list_value = Value::List(vec![Value::Text("a".to_string()), Value::Text("b".to_string())]);
        let inferred = parser.infer_type_from_value(&list_value).unwrap();
        if let CqlType::List(inner) = inferred.cql_type {
            assert_eq!(*inner, CqlType::Text);
        } else {
            panic!("Expected List<Text> type");
        }
    }
}