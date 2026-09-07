//! FROZEN MAP decoding, and the MAP-KEY NEVER-NULL rule (#3847) — split out of
//! `frozen.rs` under the campsite rule (#1116).
//!
//! `frozen.rs` was 133 lines OVER the 800-line source threshold before #3847 went
//! near it, so the ratchet forbade ANY growth and two call sites cannot be zero
//! lines. Rather than acknowledge that with `CQLITE_ALLOW_FILE_GROWTH=1`, the map
//! decoders move here with the rule they now enforce: this module owns frozen MAP
//! decoding (`parse_frozen_map_value`, `parse_frozen_map_value_raw`) and the
//! never-null key invariant, and `frozen.rs` keeps list/set/tuple. That takes
//! `frozen.rs` under its threshold for the first time.
//!
//! # Why this module exists at all
//!
//! #3847 widened the shared fixed-width decoder so an EMPTY buffer answers
//! `Value::Null` — Cassandra's `deserialize()` maps an empty buffer to null, and
//! `parse_value_from_raw_bytes` now honours that. A VALUE may be null. A MAP KEY
//! MAY NOT: Cassandra has no way to express one, and #3747 already established the
//! answer for an empty/untypeable map key — PRESERVE IT OPAQUELY (empty blob, plus
//! a signal to the caller) rather than drop the entry or hand back an invalid key.
//!
//! # The lesson this module is the monument to
//!
//! **Widening what a shared decoder ACCEPTS changes every caller that branched on
//! its REJECTION.** That single cause produced FOUR defects on #3847, each found
//! one caller further out:
//!
//! 1. the gate of record's `core-tests` red — the cell-path key's opaque policy sat
//!    on the decoder's `Err` arm and stopped firing once the decode SUCCEEDED;
//! 2. roborev job 152 — a frozen-spelled key decodes to `Frozen(<inner>)`, so a
//!    test against the RAW value fell through it;
//! 3. roborev job 153 — BOTH frozen-map key paths (`read_frozen_element`, used by
//!    `parse_frozen_map_value`, AND `parse_frozen_map_value_raw`) could yield a
//!    null key, and only the second was reported;
//! 4. and the reason it took four rounds: each fix addressed the SITE in front of
//!    it rather than the CLASS.
//!
//! # Three properties that stop a fifth site appearing
//!
//! * ONE rule, called from BOTH frozen key paths — not the same patch twice.
//! * STATED OVER THE VALUE, NOT A BYTE LENGTH. `Null` is not a legal key however it
//!   arose, so the rule takes no length argument and therefore CANNOT miss a caller
//!   that has not got one. `read_frozen_element` is exactly such a caller.
//! * IT REUSES THE ONE PEEL HELPER (`peeled_for_inspection`, widened to
//!   `pub(in ...row_decoder)`) rather than adding a second. That helper LOOPS, so
//!   a legally re-frozen inner (`frozen<frozen<list<int>>>`, which
//!   `RawCollection::freeze` permits) is covered. A second peel implementation
//!   would be a second place for the `Frozen(Null)` case to regress independently.
//!
//! # THE PEEL IS A VALUE-SIDE PEEL, AND IT IS NOT WHAT #4104 REMOVED
//!
//! An earlier revision of this header justified the peel with "`frozen<int>`
//! decodes to `Frozen(Null)`". That spelling is now REFUSED at both metadata entry
//! points (`schema::frozen_scalar`; `CQL3Type.Raw::freeze()` throws —
//! `cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`), so
//! it can no longer be the reason. The peel stays because it is stated over the
//! VALUE and the LEGAL frozen families reach it: a `frozen<absent_udt>` key comes
//! back as `Frozen(Blob)`, and a `Frozen(..)` wrapper must not hide an invalid
//! inner from a check keyed on the inner. What #4104 removed is the TYPE-STRING
//! peel in `complex_column::cell_path_key`, which could only ever change an answer
//! for a frozen scalar.

use super::*;

impl V5CompressedLegacyParser {
    /// Every member of a FROZEN SET, through [`Self::frozen_key_never_null`].
    ///
    /// A set member is a KEY (Cassandra stores it in the cell path), so this is the
    /// set-shaped spelling of the same rule. It exists so the call sites in
    /// `frozen.rs` read as ONE self-documenting line: that file sits at its 800-line
    /// campsite threshold, and an inlined `into_iter().map(...)` costs four lines
    /// there once rustfmt has wrapped it.
    pub(super) fn frozen_set_members_never_null(&self, xs: Vec<Value>, desc: &str) -> Vec<Value> {
        xs.into_iter()
            .map(|v| self.frozen_key_never_null(v, desc))
            .collect()
    }

    /// A KEY IS NEVER `null`, whatever produced it — MAP KEYS *and* SET MEMBERS.
    ///
    /// roborev job 170 widened this from map keys alone, and the reason is the
    /// STORAGE MODEL rather than a convention: Cassandra puts a `set<T>` member in
    /// the CELL PATH (the cell's name, with an empty value), exactly as it puts a
    /// map key there — so a set member IS a key and `Set([Null])` is as
    /// unexpressible as a null map key. A `list` element, by contrast, is stored as
    /// the cell VALUE under a UUID name, so `List([Null])` is a faithful
    /// `deserialize()` answer and is left alone. The multicell set path already got
    /// this for free (it decodes members through `cell_path_key`); the FROZEN set
    /// path did not, which is the gap job 170 found.
    ///
    /// roborev job 153 (Medium). #3847 widened the shared fixed-width rule so an
    /// empty buffer answers `Value::Null`, and BOTH frozen-map key paths decode
    /// their key through that same shared decoder — `read_frozen_element` (used by
    /// `parse_frozen_map_value`) and `parse_frozen_map_value_raw` directly — so a
    /// zero-length fixed-width key produced a `Value::Map` with a NULL KEY, which
    /// Cassandra has no way to express.
    ///
    /// ONE rule called from BOTH sites, rather than the same patch applied twice.
    /// Job 152 was this defect at the cell-path key and was fixed there alone; the
    /// standing lesson is that widening what a shared decoder ACCEPTS changes every
    /// caller that branched on its REJECTION, so the whole class gets swept.
    ///
    /// Same answer #3747 established for an untypeable/empty map key: PRESERVE it
    /// opaquely. An `Err` would be worse — row assembly swallows it into a silently
    /// truncated row, the hazard `cell_path_key.rs` documents — and dropping the
    /// entry loses a key Cassandra accepts.
    ///
    /// Stated over the VALUE, not over the byte length: `Null` is not a legal key
    /// however it arose, so this needs no length argument and cannot miss a caller
    /// that has none. It PEELS first, via the same helper the cell-path `Blob`
    /// diagnostic uses — a `Frozen(Null)` is exactly as unexpressible as a bare
    /// `Null`, and a legal frozen key type (collection/tuple/UDT) is precisely what
    /// puts the wrapper there — and that helper LOOPS, so a re-frozen inner is
    /// covered.
    pub(super) fn frozen_key_never_null(&self, key: Value, desc: &str) -> Value {
        if matches!(Self::peeled_for_inspection(&key), Value::Null) {
            tracing::warn!(
                "Frozen {}: empty fixed-width map key decoded as null; preserving it \
                 opaquely as empty bytes (a map key cannot be null)",
                desc
            );
            return Value::blob(Vec::new());
        }
        key
    }

    /// Parse frozen map value.
    ///
    /// The cell layout on disk is:
    ///   [VUInt blob_len][i32 BE entry_count][i32 BE key_len][key_bytes][i32 BE val_len][val_bytes]...
    pub(super) fn parse_frozen_map_value(
        &self,
        data: &[u8],
        mut offset: usize,
        key_type: &str,
        value_type: &str,
        column: &crate::schema::Column,
    ) -> Result<(Value, usize)> {
        let (count, blob_end) = Self::read_frozen_preamble(data, &mut offset, "map", &column.name)?;

        tracing::debug!(
            "V5CompressedLegacy: Frozen map '{}' with {} entries, key_type='{}', value_type='{}'",
            column.name,
            count,
            key_type,
            value_type
        );

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let key_desc = format!("map '{}' key {}", column.name, i);
            let key_value =
                self.read_frozen_element(data, &mut offset, blob_end, key_type, &key_desc, 0)?;
            // A map key is never null (job 153) — ONE rule, both frozen key sites.
            let key_value = self.frozen_key_never_null(key_value, &key_desc);

            let val_desc = format!("map '{}' value {}", column.name, i);
            let val_value =
                self.read_frozen_element(data, &mut offset, blob_end, value_type, &val_desc, 0)?;

            tracing::debug!("Frozen map entry {i}: {key_value:?} -> {val_value:?}");
            entries.push((key_value, val_value));
        }

        Self::require_frozen_extent(offset, blob_end, "map", &column.name)?; // #3811 (F)
        Ok((Value::Map(entries), blob_end))
    }

    /// Parse frozen map value (raw version without Column parameter).
    pub(super) fn parse_frozen_map_value_raw(
        &self,
        data: &[u8],
        mut offset: usize,
        key_type: &str,
        value_type: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        let count = Self::read_frozen_count(data, &mut offset, data.len(), "map", column_name)?;

        tracing::debug!(
            "V5CompressedLegacy: Parsing frozen map '{}' with {} entries (raw)",
            column_name,
            count
        );

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            // Key: [i32 BE len][key bytes]
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': not enough bytes for key {} length",
                    column_name, i
                )));
            }
            let key_len_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            if key_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': negative key {} length {}",
                    column_name, i, key_len_i32
                )));
            }
            let key_len = key_len_i32 as usize;
            offset += 4;

            if offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': key {} needs {} bytes but only {} available",
                    column_name,
                    i,
                    key_len,
                    data.len() - offset
                )));
            }
            let key_data = &data[offset..offset + key_len];
            let key_value =
                self.parse_value_from_raw_bytes(key_data, key_type, column_name, depth)?;
            // A map key is never null (job 153) — ONE rule, both frozen key sites.
            let key_desc = format!("map '{}' key {}", column_name, i);
            let key_value = self.frozen_key_never_null(key_value, &key_desc);
            offset += key_len;

            // Value: [i32 BE len][value bytes]
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': not enough bytes for value {} length",
                    column_name, i
                )));
            }
            let val_len_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            if val_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': negative value {} length {}",
                    column_name, i, val_len_i32
                )));
            }
            let val_len = val_len_i32 as usize;
            offset += 4;

            if offset + val_len > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': value {} needs {} bytes but only {} available",
                    column_name,
                    i,
                    val_len,
                    data.len() - offset
                )));
            }
            let val_data = &data[offset..offset + val_len];
            let val_value =
                self.parse_value_from_raw_bytes(val_data, value_type, column_name, depth)?;
            offset += val_len;

            entries.push((key_value, val_value));
        }

        Ok((Value::Map(entries), offset))
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::super::test_support::helpers::*;
    #[allow(unused_imports)]
    use super::*;

    /// A FROZEN MAP KEY can never be `null`, and #3847 made that reachable.
    ///
    /// roborev job 153 (Medium). #3847 widened the shared fixed-width rule so an
    /// empty buffer answers `Value::Null`, and BOTH frozen-map key paths decode
    /// their key through that same shared decoder — `read_frozen_element` (used by
    /// `parse_frozen_map_value`) and `parse_frozen_map_value_raw` directly — so a
    /// zero-length fixed-width key produced a `Value::Map` with a NULL KEY, which
    /// Cassandra cannot express.
    ///
    /// Same policy as the multicell/cell-path key (#3747): PRESERVE the key opaquely
    /// rather than drop it or return an invalid null. An `Err` here would be worse —
    /// row assembly swallows it into a silently truncated row, the hazard
    /// `cell_path_key.rs` documents.
    #[test]
    fn an_empty_fixed_width_key_in_a_frozen_map_is_never_a_null_key() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        // [i32 count=1][i32 key_len=0][i32 val_len=4][val=7]
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes());
        data.extend_from_slice(&4i32.to_be_bytes());
        data.extend_from_slice(&7i32.to_be_bytes());

        let (value, _consumed) = parser
            .parse_frozen_map_value_raw(&data, 0, "int", "int", "m", 0)
            .expect("an empty key is what Cassandra accepts; keep the entry");
        let Value::Map(entries) = &value else {
            panic!("expected a map, got {value:?}");
        };
        assert_eq!(entries.len(), 1, "the entry must be kept");
        assert_ne!(
            entries[0].0,
            Value::Null,
            "a frozen map key must NEVER be null — Cassandra cannot express it"
        );
        assert_eq!(
            entries[0].0,
            Value::blob(Vec::new()),
            "preserved opaquely, the same answer the cell-path key gives"
        );
    }
}
