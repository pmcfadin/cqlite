//! `frozen<…>`-spelled empty fixed-width CELL-PATH KEYS (#3847) — a campsite split
//! of `cell_path_key_tests.rs` (#1135), which sits at its 1500-line threshold.
//!
//! Kept under `complex_column` rather than beside the rest of the #3847 key work in
//! `frozen_map`, because `parse_cell_path_key` is `pub(super)` HERE: a sibling module
//! cannot call it. The rule these cases pin, and the four defects one root cause
//! produced, are documented in `row_decoder::frozen_map`.

// #3805/#4017 CROSS-LANE COLLISION, RULED BY THE LEAD ON PR #4033: this module's
// only case (`an_empty_frozen_spelled_fixed_width_key_is_also_preserved_opaquely`,
// asserting `Blob(b"")` + `opaque_out`) and #3805's opposite pin (asserting
// `Empty(Int)`) were BOTH DELETED. The module is kept as the record, because this
// is where a reader looking for the frozen-spelled key cases will come.
//
// The oracle is Cassandra's GRAMMAR, not its bytes. `CQL3Type.Raw::freeze()` throws
// "frozen<> is only allowed on collections, tuples, and user-defined types"
// (cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651), and
// only RawCollection/RawTuple/RawUT override it — so no table can carry
// `frozen<int>`, no serialization header can spell `FrozenType(Int32Type)`, and no
// Cassandra-written bytes for this input exist BY CONSTRUCTION. Under #28, where
// Cassandra has no behaviour CQLite must not invent one, so BOTH answers were
// inventions and the correct behaviour is REFUSAL.
//
// The refusal is its own oracle-driven fix, **#4104** (refuse `frozen<scalar>` at
// schema-parse and header-parse), deliberately NOT bundled into #4033.
//
// WHAT SURVIVES UNTOUCHED: #4017's DOOR-2 fix itself — keying the
// decode-succeeded-with-`Null` check on the PEELED probe rather than on `decoded`.
// That is still load-bearing for every family the tag table does NOT admit, which
// #3805's admission gate never intercepts. Its argument, and the four defects one
// root cause produced, are documented in `row_decoder::frozen_map`.
