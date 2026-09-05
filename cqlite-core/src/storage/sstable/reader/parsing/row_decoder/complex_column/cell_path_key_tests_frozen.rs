//! `frozen<…>`-spelled empty fixed-width CELL-PATH KEYS (#3847) — a campsite split
//! of `cell_path_key_tests.rs` (#1135), which sits at its 1500-line threshold.
//!
//! Kept under `complex_column` rather than beside the rest of the #3847 key work in
//! `frozen_map`, because `parse_cell_path_key` is `pub(super)` HERE: a sibling module
//! cannot call it. The rule these cases pin, and the four defects one root cause
//! produced, are documented in `row_decoder::frozen_map`.

#[cfg(test)]
mod tests {
    use super::super::*;

    /// A FROZEN-SPELLED empty fixed-width key gets the SAME opaque answer — the check
    /// must look THROUGH the wrapper.
    ///
    /// roborev job 152 (Medium) on this branch. `frozen<int>` decodes to
    /// `Value::Frozen(Box::new(Value::Null))`, so a `matches!(decoded, Value::Null)`
    /// test — which is what the first version of door 2 used — sees a `Frozen` and
    /// falls through, returning an invalid logical NULL map key. The peeled `probe`
    /// already exists three lines below for exactly this reason (the `Blob` diagnostic
    /// learned it in #3612 round 8), and door 2 has to use it too.
    ///
    /// `peeled_for_inspection` loops, so NESTING is covered rather than just one layer.
    #[test]
    fn an_empty_frozen_spelled_fixed_width_key_is_also_preserved_opaquely() {
        let p = V5CompressedLegacyParser::new("ks".to_string(), "t".to_string(), 0, 0, None);
        #[rustfmt::skip]
        let types = [
            "frozen<int>", "frozen<bigint>", "frozen<uuid>", "frozen<boolean>",
            "frozen<frozen<int>>",
        ];
        for type_str in types {
            let mut opaque = false;
            let decoded = p
                .parse_cell_path_key_reporting(&[], type_str, "k", &mut opaque)
                .unwrap_or_else(|e| panic!("{type_str}: Cassandra accepts empty; keep it: {e}"));
            assert_eq!(
                decoded,
                Value::blob(Vec::new()),
                "{type_str}: a frozen-spelled empty key must be preserved opaquely, \
                 never returned as a null map key"
            );
            assert!(
                opaque,
                "{type_str}: opaque_out must be raised through the frozen wrapper"
            );
        }
    }
}
