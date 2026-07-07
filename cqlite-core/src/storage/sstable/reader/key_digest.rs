//! Partition key digest computation for Index.db lookups

use crate::{schema::registry::ParsingContext, Result};

use super::super::key_digest::KeyDigestComputer;
use super::types::SSTableReader;

impl SSTableReader {
    /// Compute partition key digest for Index.db lookup.
    ///
    /// This method computes the digest from raw partition key bytes using the appropriate
    /// byte-comparable encoding based on the table schema's partition key definition.
    ///
    /// For modern formats (BIG v5, BTI), this MUST use schema-driven Murmur3 digest.
    /// Simple digest is only allowed for legacy formats behind feature flag.
    ///
    /// NOTE (Issue #553): This function is no longer called from the main
    /// `lookup_partition_with_index` path, which now uses raw key bytes directly.
    /// It is retained for potential future use and for `lookup_partition_with_schema_context`.
    #[allow(dead_code)]
    pub(crate) async fn compute_partition_key_digest(
        &self,
        partition_key: &[u8],
    ) -> Result<Vec<u8>> {
        let mut computer = KeyDigestComputer::new();

        // For modern formats, schema-driven digest is MANDATORY
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig
            | crate::parser::header::CassandraVersion::V5_0Bti => {
                // Modern formats require schema registry and must never use simple digest
                if let Some(schema_registry) = &self.schema_registry {
                    // Acquire read lock to access SchemaRegistry methods
                    #[cfg(feature = "state_machine")]
                    let schema_guard = schema_registry.read().await;
                    #[cfg(not(feature = "state_machine"))]
                    let schema_guard = schema_registry.as_ref();

                    match schema_guard
                        .get_parsing_context(&self.header.keyspace, &self.header.table_name)
                        .await
                    {
                        Ok(parsing_context) => {
                            tracing::debug!(
                                "Using schema-driven Murmur3 digest for modern format {:?}",
                                self.header.cassandra_version
                            );
                            return computer
                                .compute_partition_key_digest(partition_key, &parsing_context);
                        }
                        Err(e) => {
                            return Err(crate::error::Error::schema(format!(
                                "Failed to get parsing context for schema-driven digest in modern format {:?}: {}. \
                                 Schema-driven digest is mandatory for modern formats.",
                                self.header.cassandra_version, e
                            )));
                        }
                    }
                } else {
                    return Err(crate::error::Error::schema(format!(
                        "Schema registry required for Index.db lookups in modern format {:?}. \
                         Modern formats cannot use simple digest fallback.",
                        self.header.cassandra_version
                    )));
                }
            }
            _ => {
                // Legacy formats: try schema-driven first, fallback to simple digest if needed
                if let Some(schema_registry) = &self.schema_registry {
                    // Acquire read lock to access SchemaRegistry methods
                    #[cfg(feature = "state_machine")]
                    let schema_guard = schema_registry.read().await;
                    #[cfg(not(feature = "state_machine"))]
                    let schema_guard = schema_registry.as_ref();

                    match schema_guard
                        .get_parsing_context(&self.header.keyspace, &self.header.table_name)
                        .await
                    {
                        Ok(parsing_context) => {
                            tracing::debug!("Using schema-driven Murmur3 digest for legacy format");
                            return computer
                                .compute_partition_key_digest(partition_key, &parsing_context);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to get parsing context for legacy format, falling back to simple digest: {}",
                                e
                            );
                            // Continue to legacy fallback below
                        }
                    }
                }
            }
        }

        // Legacy path: only allow simple digest when legacy-heuristics feature is enabled
        #[cfg(feature = "legacy-heuristics")]
        {
            tracing::warn!("Falling back to simple digest - this may cause incorrect Index.db lookups");
            computer.compute_simple_digest(partition_key)
        }

        #[cfg(not(feature = "legacy-heuristics"))]
        {
            // For modern builds, reject simple digest to force schema-driven approach
            Err(crate::error::Error::invalid_operation(
                "Schema-driven digest required for Index.db lookup in modern format. \
                 Enable 'legacy-heuristics' feature only for legacy compatibility testing.",
            ))
        }
    }

    /// Compute partition key digest using table schema comparator
    ///
    /// This is the proper implementation that should be used when table schema
    /// is available. It uses the partition key comparator to create the exact
    /// digest that Cassandra would store in Index.db.
    pub(crate) fn compute_partition_key_digest_with_schema(
        &self,
        partition_key: &[u8],
        parsing_context: &ParsingContext,
    ) -> Result<Vec<u8>> {
        let mut computer = KeyDigestComputer::new();

        // Use the proper schema-driven digest computation
        computer.compute_partition_key_digest(partition_key, parsing_context)
    }
}
