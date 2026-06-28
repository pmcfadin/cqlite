//! Comparator-type derivation and compatibility checks for [`TableSchema`].
//!
//! Houses the `TableSchema` methods that map columns to [`ComparatorType`]s and
//! decide whether two comparators are compatible. Extracted from
//! `schema/mod.rs` (issue #1134, source-split doctrine) with no behavior change.

use super::{CqlType, TableSchema};
use crate::error::{Error, Result};
use crate::types::ComparatorType;
use std::collections::HashMap;

impl TableSchema {
    /// Get ComparatorType for a specific column
    pub fn get_column_comparator(&self, column_name: &str) -> Result<ComparatorType> {
        let column = self
            .get_column(column_name)
            .ok_or_else(|| Error::Schema(format!("Column '{}' not found", column_name)))?;

        let cql_type = CqlType::parse(&column.data_type)?;
        ComparatorType::from_cql_type(&cql_type)
    }

    /// Get ComparatorTypes for all columns
    pub fn get_all_comparators(&self) -> Result<HashMap<String, ComparatorType>> {
        let mut comparators = HashMap::new();

        for column in &self.columns {
            let cql_type = CqlType::parse(&column.data_type)?;
            let comparator = ComparatorType::from_cql_type(&cql_type)?;
            comparators.insert(column.name.clone(), comparator);
        }

        Ok(comparators)
    }

    /// Get ComparatorTypes for partition key columns in order
    pub fn get_partition_key_comparators(&self) -> Result<Vec<ComparatorType>> {
        let mut comparators = Vec::new();
        let ordered_keys = self.ordered_partition_keys();

        for key_column in ordered_keys {
            let cql_type = CqlType::parse(&key_column.data_type)?;
            let comparator = ComparatorType::from_cql_type(&cql_type)?;
            comparators.push(comparator);
        }

        Ok(comparators)
    }

    /// Get ComparatorTypes for clustering key columns in order
    pub fn get_clustering_key_comparators(&self) -> Result<Vec<ComparatorType>> {
        let mut comparators = Vec::new();
        let ordered_keys = self.ordered_clustering_keys();

        for key_column in ordered_keys {
            let cql_type = CqlType::parse(&key_column.data_type)?;
            let comparator = ComparatorType::from_cql_type(&cql_type)?;
            comparators.push(comparator);
        }

        Ok(comparators)
    }

    /// Check if a column type is compatible with an expected type
    pub fn is_column_type_compatible(
        &self,
        column_name: &str,
        expected_type: &str,
    ) -> Result<bool> {
        let column_comparator = self.get_column_comparator(column_name)?;
        let expected_cql_type = CqlType::parse(expected_type)?;
        let expected_comparator = ComparatorType::from_cql_type(&expected_cql_type)?;

        Ok(self.comparators_are_compatible(&column_comparator, &expected_comparator))
    }

    /// Check if two ComparatorTypes are compatible (helper method)
    #[allow(clippy::only_used_in_recursion)]
    fn comparators_are_compatible(&self, left: &ComparatorType, right: &ComparatorType) -> bool {
        match (left, right) {
            // Exact matches
            (ComparatorType::Boolean, ComparatorType::Boolean) => true,
            (ComparatorType::TinyInt, ComparatorType::TinyInt) => true,
            (ComparatorType::SmallInt, ComparatorType::SmallInt) => true,
            (ComparatorType::Int, ComparatorType::Int) => true,
            (ComparatorType::BigInt, ComparatorType::BigInt) => true,
            (ComparatorType::Float32, ComparatorType::Float32) => true,
            (ComparatorType::Float, ComparatorType::Float) => true,
            (ComparatorType::Text, ComparatorType::Text) => true,
            (ComparatorType::Blob, ComparatorType::Blob) => true,
            (ComparatorType::Timestamp, ComparatorType::Timestamp) => true,
            (ComparatorType::Uuid, ComparatorType::Uuid) => true,
            (ComparatorType::Json, ComparatorType::Json) => true,

            // Collection types
            (ComparatorType::List(l_elem), ComparatorType::List(r_elem)) => {
                self.comparators_are_compatible(l_elem, r_elem)
            }
            (ComparatorType::Set(l_elem), ComparatorType::Set(r_elem)) => {
                self.comparators_are_compatible(l_elem, r_elem)
            }
            (ComparatorType::Map(l_key, l_val), ComparatorType::Map(r_key, r_val)) => {
                self.comparators_are_compatible(l_key, r_key)
                    && self.comparators_are_compatible(l_val, r_val)
            }

            // Tuple types
            (ComparatorType::Tuple(l_fields), ComparatorType::Tuple(r_fields)) => {
                l_fields.len() == r_fields.len()
                    && l_fields
                        .iter()
                        .zip(r_fields.iter())
                        .all(|(l, r)| self.comparators_are_compatible(l, r))
            }

            // UDT types
            (
                ComparatorType::Udt {
                    type_name: l_name,
                    keyspace: l_ks,
                    ..
                },
                ComparatorType::Udt {
                    type_name: r_name,
                    keyspace: r_ks,
                    ..
                },
            ) => l_name == r_name && l_ks == r_ks,

            // Frozen types
            (ComparatorType::Frozen(l_inner), ComparatorType::Frozen(r_inner)) => {
                self.comparators_are_compatible(l_inner, r_inner)
            }

            // Custom types
            (ComparatorType::Custom(l_name), ComparatorType::Custom(r_name)) => l_name == r_name,

            // No other combinations are compatible
            _ => false,
        }
    }
}
