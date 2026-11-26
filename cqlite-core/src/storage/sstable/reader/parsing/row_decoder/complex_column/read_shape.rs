//! Issue #4645: the READ shape of a decoded non-frozen collection.
//!
//! One decode loop (`parse_complex_column_impl`) serves two consumers. Physical ones
//! (compaction, delta-scan, unit tests) call [`V5CompressedLegacyParser::parse_complex_column_inner`]
//! and see the on-disk shape: a deleted map entry is `(key, Null)`, a list is in on-disk order.
//! The user-facing SELECT decode (`parse_complex_column`) passes `user_read = true` and gets
//! Cassandra's read semantics, the ones `merge::read_assembly` produces for a merged read:
//! a deleted element is omitted (`ComplexColumnData` only iterates live cells), and a list is
//! in `ListType` order, i.e. TimeUUID order of the cell path (`timeuuid_path_cmp`, the ONE
//! shared comparator the assembler uses). Cassandra re-orders by the comparator on read, so a
//! generation whose cells are not in comparator order (CQLite's own writer before #4643)
//! must still read in comparator order.

use super::super::*;

impl V5CompressedLegacyParser {
    /// Physical decode of one complex column: on-disk shape, no read-side shaping.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn parse_complex_column_inner(
        &self,
        data: &[u8],
        offset: usize,
        column: &crate::schema::Column,
        complex_type: &str,
        has_complex_deletion: bool,
        row_timestamp: i64,
        elements_out: Option<
            &mut Vec<crate::storage::sstable::reader::compaction_row::ComplexElement>,
        >,
        element_filter: Option<ElementShadow>,
    ) -> Result<(Value, usize, ComplexColumnMeta)> {
        self.parse_complex_column_impl(
            data,
            offset,
            column,
            complex_type,
            has_complex_deletion,
            row_timestamp,
            elements_out,
            element_filter,
            false,
        )
    }

    /// Issue #1741 (per-element filtering): whether a parsed non-frozen collection
    /// value emitted ZERO surviving elements (so the read path treats it as absent /
    /// null). Only the collection variants are meaningful; any other value returns
    /// `false` (never treated as an empty collection).
    pub(crate) fn complex_value_is_empty(value: &Value) -> bool {
        match value {
            Value::List(v) => v.is_empty(),
            Value::Set(v) => v.is_empty(),
            Value::Map(v) => v.is_empty(),
            Value::Udt(u) => u.fields.iter().all(|f| f.value.is_none()),
            _ => false,
        }
    }
}

/// The list value from its `(cell_path, element)` pairs. A SELECT read orders them by the
/// shared TimeUUID path comparator (stable, so equal paths keep on-disk order); the physical
/// decode keeps on-disk order.
pub(super) fn finish_list(mut elements: Vec<(Vec<u8>, Value)>, user_read: bool) -> Vec<Value> {
    if user_read {
        elements.sort_by(|a, b| crate::uuid_order::timeuuid_path_cmp(&a.0, &b.0));
    }
    elements.into_iter().map(|(_, v)| v).collect()
}
