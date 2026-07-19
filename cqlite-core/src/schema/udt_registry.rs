//! UDT (User Defined Type) schema registry.
//!
//! Houses [`UdtRegistry`], which stores and resolves User Defined Type
//! definitions per keyspace, including dependency validation and CREATE TYPE
//! export. Extracted from `schema/mod.rs` (issue #1134, source-split doctrine)
//! with no behavior change.

use super::CqlType;
use crate::types::UdtTypeDef;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Build a [`UdtRegistry`] from every `CREATE TYPE` statement in a CQL DDL string
/// (issue #2349).
///
/// This is the SINGLE authoritative DDL→registry resolver, reused by the CLI
/// write path (`udt_registry_from_schema_file`) and the Flight read path (which
/// resolves a ticket's DDL): it splits the DDL into statements
/// ([`super::cql_parser::split_cql_statements`]), parses each `CREATE TYPE`
/// ([`super::cql_parser::parse_create_type`]), and registers the resulting
/// [`UdtTypeDef`]. A field whose declared type does not parse falls back to
/// [`CqlType::Blob`] (rendered `BytesType`), matching the writer's unknown-type
/// handling. A `CREATE TYPE` with no explicit keyspace inherits `default_keyspace`.
///
/// No-heuristics (issue #28): types come ONLY from the authoritative DDL — never
/// inferred from data bytes. A DDL carrying no `CREATE TYPE` yields an empty
/// registry (resolution then a no-op), so a non-UDT table is unaffected.
pub fn udt_registry_from_cql(cql: &str, default_keyspace: &str) -> UdtRegistry {
    use super::cql_parser::{parse_create_type, split_cql_statements};

    let mut registry = UdtRegistry::new();
    for stmt in split_cql_statements(cql) {
        if let Ok((_, (name, keyspace, fields))) = parse_create_type(&stmt) {
            let keyspace = keyspace.unwrap_or_else(|| default_keyspace.to_string());
            let mut def = UdtTypeDef::new(keyspace, name);
            for (field_name, field_type) in fields {
                let cql = CqlType::parse(&field_type).unwrap_or(CqlType::Blob);
                def = def.with_field(field_name, cql, true);
            }
            registry.register_udt(def);
        }
    }
    registry
}

/// UDT Schema Registry for managing User Defined Type definitions
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UdtRegistry {
    /// Registered UDT type definitions by keyspace and type name
    udts: HashMap<String, HashMap<String, UdtTypeDef>>,
}

impl UdtRegistry {
    /// Create a new UDT registry
    pub fn new() -> Self {
        Self {
            udts: HashMap::new(),
        }
    }

    /// Create a new UDT registry with enhanced Cassandra 5.0 defaults
    pub fn with_cassandra5_defaults() -> Self {
        let mut registry = Self::new();
        registry.load_cassandra5_system_udts();
        registry
    }

    /// Register a UDT type definition
    pub fn register_udt(&mut self, udt_def: UdtTypeDef) {
        let keyspace_udts = self.udts.entry(udt_def.keyspace.clone()).or_default();
        keyspace_udts.insert(udt_def.name.clone(), udt_def);
    }

    /// Get a UDT definition by keyspace and name
    pub fn get_udt(&self, keyspace: &str, name: &str) -> Option<&UdtTypeDef> {
        self.udts.get(keyspace)?.get(name)
    }

    /// Get all UDTs in a keyspace
    pub fn get_keyspace_udts(&self, keyspace: &str) -> Option<&HashMap<String, UdtTypeDef>> {
        self.udts.get(keyspace)
    }

    /// List all registered UDT names in a keyspace
    pub fn list_udt_names(&self, keyspace: &str) -> Vec<&str> {
        self.udts
            .get(keyspace)
            .map(|udts| udts.keys().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Check if a UDT is registered
    pub fn contains_udt(&self, keyspace: &str, name: &str) -> bool {
        self.udts
            .get(keyspace)
            .map(|udts| udts.contains_key(name))
            .unwrap_or(false)
    }

    /// Remove a UDT definition
    pub fn remove_udt(&mut self, keyspace: &str, name: &str) -> Option<UdtTypeDef> {
        self.udts.get_mut(keyspace)?.remove(name)
    }

    /// Clear all UDTs in a keyspace
    pub fn clear_keyspace(&mut self, keyspace: &str) {
        self.udts.remove(keyspace);
    }

    /// Get total number of registered UDTs
    pub fn total_udts(&self) -> usize {
        self.udts.values().map(|udts| udts.len()).sum()
    }

    /// Load enhanced Cassandra 5.0 system UDTs with complex nested structures
    fn load_cassandra5_system_udts(&mut self) {
        // Enhanced address UDT for Cassandra 5.0 compatibility
        let address_udt = UdtTypeDef::new("system".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("street2".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true)
            .with_field("state".to_string(), CqlType::Text, true)
            .with_field("zip_code".to_string(), CqlType::Text, true)
            .with_field("country".to_string(), CqlType::Text, true)
            .with_field(
                "coordinates".to_string(),
                CqlType::Tuple(vec![CqlType::Double, CqlType::Double]),
                true,
            );

        self.register_udt(address_udt);

        // Enhanced person UDT with collections and nested types
        let person_udt = UdtTypeDef::new("system".to_string(), "person".to_string())
            .with_field("id".to_string(), CqlType::Uuid, false)
            .with_field("first_name".to_string(), CqlType::Text, false)
            .with_field("last_name".to_string(), CqlType::Text, false)
            .with_field("middle_name".to_string(), CqlType::Text, true)
            .with_field("age".to_string(), CqlType::Int, true)
            .with_field("email".to_string(), CqlType::Text, true)
            .with_field(
                "phone_numbers".to_string(),
                CqlType::Set(Box::new(CqlType::Text)),
                true,
            )
            .with_field(
                "addresses".to_string(),
                CqlType::List(Box::new(CqlType::Udt("address".to_string(), vec![]))),
                true,
            )
            .with_field(
                "metadata".to_string(),
                CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Text)),
                true,
            );

        self.register_udt(person_udt);

        // Contact info UDT for complex nested scenarios
        let contact_info_udt = UdtTypeDef::new("system".to_string(), "contact_info".to_string())
            .with_field(
                "person".to_string(),
                CqlType::Udt("person".to_string(), vec![]),
                false,
            )
            .with_field(
                "primary_address".to_string(),
                CqlType::Udt("address".to_string(), vec![]),
                true,
            )
            .with_field(
                "emergency_contacts".to_string(),
                CqlType::List(Box::new(CqlType::Udt("person".to_string(), vec![]))),
                true,
            )
            .with_field("last_updated".to_string(), CqlType::Timestamp, true);

        self.register_udt(contact_info_udt);
    }

    /// Rewrite every UDT reference inside `ty` into a fully-structured
    /// [`CqlType::Udt`] whose fields come from this registry (issue #2349).
    ///
    /// `CqlType::parse` renders a UDT reference as `Custom("udt:<name>")` (or a
    /// bare `Custom("<name>")` for lowercase names) with NO field information, and
    /// an already-`Udt` node with an empty field list is likewise unresolved. This
    /// walks the type tree and replaces each such reference with the registry's
    /// authoritative field definitions, recursing through collection/frozen/tuple
    /// wrappers and into each resolved UDT field's own type (so `frozen<contact>`
    /// with an inner `frozen<address>` field fully materializes).
    ///
    /// A reference with no registry entry is left UNCHANGED — resolution is a
    /// no-op fail-open, never a fabricated type (no-heuristics, issue #28). The
    /// lookup honours an explicit `keyspace.type` qualifier and falls back to the
    /// `system` keyspace, mirroring [`super::TableSchema::validate_udt_references`].
    pub fn resolve_type(&self, ty: &CqlType, keyspace: &str) -> CqlType {
        // Cap recursion so a (registry-level) cyclic UDT reference can never
        // loop forever; Cassandra forbids cycles, so a real schema is shallow.
        self.resolve_type_depth(ty, keyspace, 0)
    }

    fn resolve_type_depth(&self, ty: &CqlType, keyspace: &str, depth: usize) -> CqlType {
        const MAX_DEPTH: usize = 32;
        if depth >= MAX_DEPTH {
            return ty.clone();
        }
        match ty {
            CqlType::List(inner) => {
                CqlType::List(Box::new(self.resolve_type_depth(inner, keyspace, depth + 1)))
            }
            CqlType::Set(inner) => {
                CqlType::Set(Box::new(self.resolve_type_depth(inner, keyspace, depth + 1)))
            }
            CqlType::Frozen(inner) => {
                CqlType::Frozen(Box::new(self.resolve_type_depth(inner, keyspace, depth + 1)))
            }
            CqlType::Map(k, v) => CqlType::Map(
                Box::new(self.resolve_type_depth(k, keyspace, depth + 1)),
                Box::new(self.resolve_type_depth(v, keyspace, depth + 1)),
            ),
            CqlType::Tuple(types) => CqlType::Tuple(
                types
                    .iter()
                    .map(|t| self.resolve_type_depth(t, keyspace, depth + 1))
                    .collect(),
            ),
            CqlType::Udt(name, fields) if fields.is_empty() => self
                .resolve_udt_reference(name, keyspace, depth)
                .unwrap_or_else(|| ty.clone()),
            CqlType::Custom(name) => {
                let udt_name = name.strip_prefix("udt:").unwrap_or(name);
                if super::is_udt_identifier(udt_name) {
                    self.resolve_udt_reference(udt_name, keyspace, depth)
                        .unwrap_or_else(|| ty.clone())
                } else {
                    ty.clone()
                }
            }
            // Already-resolved UDTs (non-empty fields) and primitives pass through.
            other => other.clone(),
        }
    }

    /// Look a UDT reference up in this registry and return its fully-resolved
    /// [`CqlType::Udt`] (fields recursively resolved), or `None` when absent.
    fn resolve_udt_reference(
        &self,
        udt_name: &str,
        keyspace: &str,
        depth: usize,
    ) -> Option<CqlType> {
        let (lookup_keyspace, bare_name) = match udt_name.split_once('.') {
            Some((ks, n)) => (ks, n),
            None => (keyspace, udt_name),
        };
        let def = self
            .get_udt(lookup_keyspace, bare_name)
            .or_else(|| self.get_udt("system", bare_name))?;
        let fields = def
            .fields
            .iter()
            .map(|f| {
                (
                    f.name.clone(),
                    // Resolve nested UDT fields against the UDT's OWN keyspace.
                    self.resolve_type_depth(&f.field_type, &def.keyspace, depth + 1),
                )
            })
            .collect();
        Some(CqlType::Udt(bare_name.to_string(), fields))
    }

    /// Resolve UDT with full dependency chain
    pub fn resolve_udt_with_dependencies(
        &self,
        keyspace: &str,
        name: &str,
    ) -> crate::Result<&UdtTypeDef> {
        let udt = self.get_udt(keyspace, name).ok_or_else(|| {
            crate::Error::schema(format!(
                "UDT '{}' not found in keyspace '{}'",
                name, keyspace
            ))
        })?;

        // Validate all field dependencies exist
        for field in &udt.fields {
            self.validate_field_type_dependencies(&field.field_type, keyspace)?;
        }

        Ok(udt)
    }

    /// Validate that all UDT field type dependencies exist in the registry
    fn validate_field_type_dependencies(
        &self,
        field_type: &CqlType,
        keyspace: &str,
    ) -> crate::Result<()> {
        match field_type {
            CqlType::Udt(udt_name, _) => {
                if !self.contains_udt(keyspace, udt_name) {
                    return Err(crate::Error::schema(format!(
                        "UDT dependency '{}' not found in keyspace '{}'",
                        udt_name, keyspace
                    )));
                }
            }
            CqlType::List(inner) | CqlType::Set(inner) | CqlType::Frozen(inner) => {
                self.validate_field_type_dependencies(inner, keyspace)?;
            }
            CqlType::Map(key_type, value_type) => {
                self.validate_field_type_dependencies(key_type, keyspace)?;
                self.validate_field_type_dependencies(value_type, keyspace)?;
            }
            CqlType::Tuple(field_types) => {
                for tuple_field_type in field_types {
                    self.validate_field_type_dependencies(tuple_field_type, keyspace)?;
                }
            }
            _ => {} // Primitive types don't need validation
        }
        Ok(())
    }

    /// Get all UDTs that depend on a given UDT (for cascade operations)
    pub fn get_dependent_udts(&self, keyspace: &str, udt_name: &str) -> Vec<&UdtTypeDef> {
        let mut dependents = Vec::new();

        if let Some(keyspace_udts) = self.udts.get(keyspace) {
            for udt in keyspace_udts.values() {
                if udt.name == udt_name {
                    continue; // Skip self
                }

                // Check if this UDT depends on the target UDT
                if self.udt_depends_on(udt, udt_name) {
                    dependents.push(udt);
                }
            }
        }

        dependents
    }

    /// Check if a UDT depends on another UDT (recursively)
    fn udt_depends_on(&self, udt: &UdtTypeDef, target_udt: &str) -> bool {
        for field in &udt.fields {
            if self.field_type_depends_on(&field.field_type, target_udt) {
                return true;
            }
        }
        false
    }

    /// Check if a field type depends on a UDT
    #[allow(clippy::only_used_in_recursion)]
    fn field_type_depends_on(&self, field_type: &CqlType, target_udt: &str) -> bool {
        match field_type {
            CqlType::Udt(udt_name, _) => udt_name == target_udt,
            CqlType::List(inner) | CqlType::Set(inner) | CqlType::Frozen(inner) => {
                self.field_type_depends_on(inner, target_udt)
            }
            CqlType::Map(key_type, value_type) => {
                self.field_type_depends_on(key_type, target_udt)
                    || self.field_type_depends_on(value_type, target_udt)
            }
            CqlType::Tuple(field_types) => field_types
                .iter()
                .any(|ft| self.field_type_depends_on(ft, target_udt)),
            _ => false,
        }
    }

    /// Register UDT with dependency validation
    pub fn register_udt_with_validation(&mut self, udt_def: UdtTypeDef) -> crate::Result<()> {
        // Validate dependencies exist
        for field in &udt_def.fields {
            self.validate_field_type_dependencies(&field.field_type, &udt_def.keyspace)?;
        }

        // Check for circular dependencies
        if self.would_create_circular_dependency(&udt_def) {
            return Err(crate::Error::schema(format!(
                "Registering UDT '{}' would create circular dependency",
                udt_def.name
            )));
        }

        self.register_udt(udt_def);
        Ok(())
    }

    /// Check if registering a UDT would create circular dependencies
    fn would_create_circular_dependency(&self, udt_def: &UdtTypeDef) -> bool {
        // This is complex - for now, just check direct self-reference
        for field in &udt_def.fields {
            if self.field_type_depends_on(&field.field_type, &udt_def.name) {
                return true;
            }
        }
        false
    }

    /// Export UDT definitions for debugging
    pub fn export_definitions(&self, keyspace: &str) -> Vec<String> {
        let mut definitions = Vec::new();

        if let Some(keyspace_udts) = self.udts.get(keyspace) {
            for udt in keyspace_udts.values() {
                let mut def = format!("CREATE TYPE {}.{} (\n", keyspace, udt.name);

                for (i, field) in udt.fields.iter().enumerate() {
                    if i > 0 {
                        def.push_str(",\n");
                    }
                    def.push_str(&format!(
                        "  {} {}",
                        field.name,
                        self.format_cql_type(&field.field_type)
                    ));
                }

                def.push_str("\n);");
                definitions.push(def);
            }
        }

        definitions
    }

    /// Format CQL type for CREATE TYPE statements
    #[allow(clippy::only_used_in_recursion)]
    fn format_cql_type(&self, cql_type: &CqlType) -> String {
        match cql_type {
            CqlType::Boolean => "boolean".to_string(),
            CqlType::TinyInt => "tinyint".to_string(),
            CqlType::SmallInt => "smallint".to_string(),
            CqlType::Int => "int".to_string(),
            CqlType::BigInt => "bigint".to_string(),
            CqlType::Counter => "counter".to_string(),
            CqlType::Float => "float".to_string(),
            CqlType::Double => "double".to_string(),
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
            CqlType::Varint => "varint".to_string(),
            CqlType::Decimal => "decimal".to_string(),
            CqlType::List(inner) => format!("list<{}>", self.format_cql_type(inner)),
            CqlType::Set(inner) => format!("set<{}>", self.format_cql_type(inner)),
            CqlType::Map(key, value) => format!(
                "map<{}, {}>",
                self.format_cql_type(key),
                self.format_cql_type(value)
            ),
            CqlType::Udt(name, _) => name.clone(),
            CqlType::Tuple(types) => {
                let type_strs: Vec<String> =
                    types.iter().map(|t| self.format_cql_type(t)).collect();
                format!("tuple<{}>", type_strs.join(", "))
            }
            CqlType::Frozen(inner) => format!("frozen<{}>", self.format_cql_type(inner)),
            CqlType::Custom(name) => name.clone(),
        }
    }
}

#[cfg(test)]
mod resolve_tests {
    use super::*;

    const DDL: &str = "\
CREATE TYPE ks.address_type (street text, city text); \
CREATE TYPE ks.contact_info (email text, address frozen<address_type>);";

    #[test]
    fn from_cql_registers_every_create_type() {
        let reg = udt_registry_from_cql(DDL, "ks");
        assert!(reg.contains_udt("ks", "address_type"));
        assert!(reg.contains_udt("ks", "contact_info"));
        assert_eq!(reg.total_udts(), 2);
    }

    #[test]
    fn from_cql_no_create_type_is_empty() {
        let reg = udt_registry_from_cql("CREATE TABLE ks.t (id int PRIMARY KEY, v text)", "ks");
        assert_eq!(reg.total_udts(), 0);
    }

    #[test]
    fn resolve_type_rewrites_custom_udt_in_list_to_struct() {
        let reg = udt_registry_from_cql(DDL, "ks");
        // `list<frozen<address_type>>` parses to List(Frozen(Custom("udt:address_type"))).
        let parsed = CqlType::parse("list<frozen<address_type>>").unwrap();
        let resolved = reg.resolve_type(&parsed, "ks");
        match &resolved {
            CqlType::List(inner) => match inner.as_ref() {
                CqlType::Frozen(udt) => match udt.as_ref() {
                    CqlType::Udt(name, fields) => {
                        assert_eq!(name, "address_type");
                        assert_eq!(fields.len(), 2, "street + city resolved from the registry");
                        assert_eq!(fields[0].0, "street");
                    }
                    other => panic!("inner must resolve to Udt, got {other:?}"),
                },
                other => panic!("expected Frozen wrapper, got {other:?}"),
            },
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn resolve_type_recurses_into_nested_udt_fields() {
        let reg = udt_registry_from_cql(DDL, "ks");
        // contact_info.address is itself a frozen<address_type> — the nested UDT
        // must resolve to a full Struct, not stay a bare Custom.
        let parsed = CqlType::parse("frozen<contact_info>").unwrap();
        let resolved = reg.resolve_type(&parsed, "ks");
        let inner = match &resolved {
            CqlType::Frozen(inner) => inner.as_ref(),
            other => panic!("expected Frozen, got {other:?}"),
        };
        let fields = match inner {
            CqlType::Udt(_, fields) => fields,
            other => panic!("expected Udt, got {other:?}"),
        };
        let (_, addr_type) = fields.iter().find(|(n, _)| n == "address").expect("address field");
        match addr_type {
            CqlType::Frozen(a) => assert!(
                matches!(a.as_ref(), CqlType::Udt(n, f) if n == "address_type" && f.len() == 2),
                "nested address field must resolve to the full address_type Struct"
            ),
            CqlType::Udt(n, f) => {
                assert_eq!(n, "address_type");
                assert_eq!(f.len(), 2);
            }
            other => panic!("nested address must resolve to a Udt, got {other:?}"),
        }
    }

    #[test]
    fn resolve_type_unknown_reference_is_left_unchanged() {
        let reg = UdtRegistry::new();
        let parsed = CqlType::parse("list<frozen<missing_type>>").unwrap();
        // No registry entry → fail-open, type tree unchanged (no fabricated fields).
        assert_eq!(reg.resolve_type(&parsed, "ks"), parsed);
    }

    #[test]
    fn resolve_type_leaves_primitives_untouched() {
        let reg = udt_registry_from_cql(DDL, "ks");
        let parsed = CqlType::parse("map<text, int>").unwrap();
        assert_eq!(reg.resolve_type(&parsed, "ks"), parsed);
    }
}
