// Standalone Gradle build for the compaction-parity harness (issue #842).
//
// This is intentionally NOT part of the Cargo workspace: it is a JVM project that
// builds against a pinned Apache Cassandra 5.0.2 source checkout (see
// scripts/bootstrap-cassandra.sh) to reach test-tree classes (CQLTester,
// CompactionTask, JsonTransformer) that Cassandra does not publish to Maven.
rootProject.name = "compaction-parity"
