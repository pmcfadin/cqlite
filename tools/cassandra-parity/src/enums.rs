//! Canonical closed value sets for the parity manifest.
//!
//! These mirror `test-data/cassandra-parity-manifest.schema.json` and the
//! taxonomy in `docs/reports/cassandra-test-parity-assessment.md`. A test
//! (`schema_enums_match`) asserts these stay in sync with the schema file.

pub const STATUS: &[&str] = &["mirrored", "partial", "planned", "out_of_scope"];

pub const CAPABILITY: &[&str] = &[
    "sstable_format",
    "component_discovery",
    "data_db_decode",
    "index_summary",
    "statistics_metadata",
    "compression_checksum",
    "corruption_verify",
    "filter_db_bloom",
    "cql_types",
    "schema_evolution",
    "tombstone_ttl",
    "delta_scan",
    "compaction_merge",
    "write_load_path",
    "bti_big_version_matrix",
    "cli_reporting",
];

pub const PRIORITY: &[&str] = &["P0", "P1", "P2"];

pub const RISK: &[&str] = &[
    "p0_data_loss",
    "p1_correctness",
    "p2_coverage",
    "node_behavior",
    "tooling_only",
];

pub const EVIDENCE_TYPE: &[&str] = &[
    "byte_for_byte",
    "canonical_semantic",
    "smoke",
    "partial",
    "out_of_scope",
];

pub const CI_TIER: &[&str] = &[
    "fast_pr",
    "required_parity",
    "nightly_docker",
    "exhaustive_regeneration",
    "manual_debug",
];

pub const SUITE: &[&str] = &[
    "sstable_parity_data_db_jsonl",
    "sstable_parity_delta_scan",
    "sstable_parity_statistics_db",
    "sstable_parity_index_db_big",
    "sstable_parity_summary_db_big",
    "sstable_parity_bti_partitions_rows",
    "sstable_parity_filter_db_bloom",
    "sstable_parity_compression_info_chunks",
    "sstable_parity_corruption_verify",
    "sstable_parity_component_manifest",
    "sstable_writer_cassandra_fixture_parity",
    "compaction_parity_tombstone_ttl",
    "schema_parity_serialization_header",
];

pub const CATEGORY: &[&str] = &[
    "sstable-format",
    "sstable-io",
    "serialization",
    "compression",
    "checksum",
    "bloom-filter",
    "compaction",
    "tombstone-ttl",
    "corruption-recovery",
    "scrub-verify",
    "commitlog",
    "memtable-flush",
    "streaming",
    "repair",
    "read-repair",
    "index-sai-sasi",
    "tools-cli",
    "other",
];

pub const RELEVANCE: &[&str] = &["high", "med", "low"];

pub const ARTIFACT: &[&str] = &[
    "bytes",
    "offsets",
    "checksums",
    "component_files",
    "jsonl",
    "logs",
    "generated_report",
];

pub const STORAGE_FORMAT: &[&str] = &["nb", "oa", "da", "big", "bti"];

pub const OUT_OF_SCOPE_CATEGORY: &[&str] = &[
    "commitlog_replay",
    "repair_coordinator",
    "read_repair_coordinator",
    "streaming_protocol",
    "node_lifecycle",
    "nodetool_jmx_metrics",
    "distributed_consensus",
    "sai_sasi_query",
    "memtable_internals",
    "java_tooling",
    "unsupported_compression_dictionary",
    "not_sstable_reader_writer_compactor",
];

/// Artifacts that count as byte-level evidence for a `byte_for_byte` claim.
pub const BYTE_LEVEL_ARTIFACTS: &[&str] = &["bytes", "offsets", "checksums", "component_files"];

/// Closed set of public-claim kinds (issue #1023). `safe` = manifest-backed
/// wording the project may publish; `blocked` = unqualified over-claim phrase the
/// claim-scan lint rejects in release-facing docs unless explicitly scoped.
pub const CLAIM_KIND: &[&str] = &["safe", "blocked"];
