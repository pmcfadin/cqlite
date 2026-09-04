//! Comparing ONE map value, golden vs CLI (issue #1491, container keys #3726).
//!
//! Split out of `golden_value_compare.rs` under the campsite rule (CLAUDE.md, epic
//! #1135), which the container-key work pushed past the ~1500-line test-file target.
//! It follows the precedent of its sibling `golden_value_compare_udt.rs`: a map is
//! the one value shape whose two sides are spelled DIFFERENTLY BY CONSTRUCTION — the
//! dump writes a JSON object, the egress a `{key,value}` array — so pairing its
//! entries is a distinct question from the structural walk in the parent module.
//!
//! Reached only through [`super::compare_value_body`], and every helper it uses
//! (`At`, `pair`, `brief`, `canon_typed`) still lives in the parent or in
//! `golden_value_canon_container`, so this file adds no surface: the call site is
//! unchanged.

use super::super::schema::CqlType;
use super::{
    brief, canon_typed, compare_value_at, container, pair, At, Canon, Depth, Egress, Kinding, Side,
};
use serde_json::{Map, Value};

/// Compare a map: golden object vs the CLI's `{key,value}` pair list, IN EMITTED
/// ORDER, each key canonicalized UNDER THE DECLARED KEY TYPE — so a `map<int,…>`
/// matches the golden's `"-5"` with the CLI's `-5`, while a `map<text,…>` compares
/// its keys exactly AND by JSON kind, so a numeric key `0` does not satisfy the
/// golden's `"0"`.
///
/// # Emitted order, because a map's order is not free
///
/// Cassandra stores a map's entries sorted by the key's comparator — a multicell
/// map's cells are keyed by `CellPath`, and a frozen map is serialized in that
/// same order — and `sstabledump` emits them in that on-disk order, which the
/// golden reader preserves (`serde_json`'s `preserve_order` is on workspace-wide,
/// and a multicell map is rebuilt cell by cell in dump order). A reader walking
/// the same SSTable therefore has no licence to emit a different order, so a
/// reordering is a DIVERGENCE.
///
/// Sorting both sides by canonicalized key before comparing (the previous rule)
/// discarded exactly that: reversing the CLI's entries compared equal, while the
/// CSV decoder's own documentation claimed member order was compared against the
/// golden (issue #1491 review finding N2). The sibling [`compare_sequence`] has
/// always compared list/set members positionally for the same reason, so nothing
/// in this walk is order-insensitive now.
///
/// The golden's keys are JSON object keys, hence always strings; the CLI's keep
/// whatever kind the egress gave them. Both go through `canon_typed(…, key_ty, …)`
/// — the golden's under [`container::golden_map_key_kinding`]'s kinding, the CLI's
/// under [`Kinding::Natural`] — which is what makes the kind comparison possible.
///
/// # A CONTAINER key type (issue #3726)
///
/// There is no separate rule for one, which is the point: `cassandra-5.0.8
/// MapType.toJSONString` writes `keys.toJSONString(kv, protocolVersion)` and quotes
/// it only when it does not already start with `"`, so a container key's JSON object
/// key is exactly that key value's own `toJSONString` document. Read through
/// [`container::golden_map_key_value`] it becomes an ordinary value of the declared
/// key type, canonicalized by the same [`canon_typed`] recursion as any other
/// position — at [`Kinding::Natural`], because `toJSONString` is the natural-kind
/// writer.
///
/// This function used to REFUSE such a key outright (`is_scalar_type`), which made
/// four columns of the committed `test_nested_udt_keys.nested_udt_keys` fixture
/// inexpressible to the lane rather than merely excluded. A refusal survives only
/// where the golden's key is not a `toJSONString` document at all — which is exactly
/// the MULTICELL case, where the key is a cell PATH and
/// `JsonTransformer.serializeCell` writes it with the OTHER writer,
/// `writeString(getString(...))` (see `gap::Divergence`).
pub(super) fn compare_map(
    golden: &Map<String, Value>,
    cli: &[Value],
    egress: Egress,
    key_ty: &CqlType,
    value_ty: &CqlType,
    at: &At<'_, '_>,
) -> Result<(), String> {
    // A key canonicalization FAILURE is propagated, never swallowed into the
    // comparison key: a `<reason>` string would still meet an identical
    // `<reason>` on the other side and compare equal.
    //
    // ASYMMETRIC, exactly as for a cell value (finding M1) and for the same
    // reason. A JSON object's key can only be a string, so the GOLDEN's map key
    // is stringified BY THE FORMAT and says nothing about kind: it is read with
    // `Kinding::Stringified`. The CLI is under no such constraint — it spells a
    // map as an ARRAY of `{"key":…,"value":…}` objects, whose `key` keeps the JSON
    // kind of its declared type — so the CLI key is held to `Kinding::Natural`,
    // i.e. to the kind its declared key type implies. Relaxing BOTH sides made a
    // regression from the `map<int,…>` key `-5` to the string `"-5"` compare equal
    // (issue #1491 review finding N1).
    //
    // The key is the `Canon` VALUE, never `Canon::describe()`: `describe` is the
    // DIAGNOSTIC rendering, and a rendering used as a comparison key can only be as
    // faithful as its spelling happens to be — the same class as finding DD1, where
    // the row-order key's `brief(&canon.describe())` made two long distinct keys
    // equal. Here the pair is compared as the structured value and rendered only
    // into the message below.
    let canon_golden_key = |key: &str| -> Result<Canon, String> {
        // WHAT THE GOLDEN'S OBJECT KEY DENOTES is asked of ONE function
        // (`container::golden_map_key_value`), which the canonical model and the CSV
        // rendering also call: for a SCALAR key type it is the key text itself, and
        // for a CONTAINER key type it is that text PARSED, because
        // `cassandra-5.0.8 MapType.toJSONString` writes
        // `keys.toJSONString(kv, protocolVersion)` and only quotes it when it does
        // not already start with `"` — so a container key's object key is exactly the
        // key value's own toJSONString document (issue #3726). Two spellings of that
        // rule would be two notions of what the golden key is.
        let value = container::golden_map_key_value(key, key_ty, at.map_key_spelling)?;
        canon_typed(
            &value,
            egress,
            key_ty,
            Depth::Inside,
            container::golden_map_key_kinding(key_ty, at.map_key_spelling),
            Side::Golden,
        )
    };
    let canon_cli_key = |v: &Value| -> Result<Canon, String> {
        canon_typed(
            v,
            egress,
            key_ty,
            Depth::Inside,
            Kinding::Natural,
            Side::Cli,
        )
    };
    // AN UNPAIRABLE KEY IS A DIVERGENCE AT THE KEY, NOT A VERDICT ON THE MAP.
    //
    // A container key on a MULTICELL column cannot be paired at all: the dump wrote it as
    // the cell PATH (`nameComparator().getString(...)`), the egress writes the raw key
    // bytes, and neither is a value the other can be canonicalized onto. Propagating that
    // as an error from here ABORTS THE WHOLE MAP — and since the declared gap then matched
    // at the column node, the entry VALUES were never walked and arbitrary value corruption
    // was suppressed with the keys (roborev job 28; measured: a value changed 90 -> 999
    // produced ZERO diffs).
    //
    // The entries are still pairable POSITIONALLY, which is not a fallback but the rule this
    // function already runs on: a map's entries are compared in EMITTED order, and both
    // sides preserve it. So each key is reported at its OWN node — where a gap declared on
    // the column is still active, because the matcher is asked at every node of the gap's
    // subtree — and the values beside them are compared like any other.
    let unpairable_keys = container::is_container_type(key_ty)
        && at.map_key_spelling == container::MapKeySpelling::GetString;
    if unpairable_keys {
        if golden.len() != cli.len() {
            return Err(format!(
                "map size golden {} vs cli {}",
                golden.len(),
                cli.len()
            ));
        }
        for (i, ((gk, gv), entry)) in golden.iter().zip(cli.iter()).enumerate() {
            let (ck, cv) = pair(entry, egress)?;
            // The KEY, at its own position. Both sides are handed over RAW: the golden's
            // object key as the text the dump wrote, the CLI's key as it stands. Neither is
            // canonicalized, because canonicalizing is exactly what cannot be done here.
            let key_at = at.map_key(i);
            compare_value_at(&Value::String(gk.clone()), ck, egress, key_ty, &key_at)
                .map_err(|why| format!("[key {i}] {why}"))?;
            // The VALUE, compared like any other map value — this is the coverage the
            // column-scoped abort used to throw away.
            let value_at = at.index(&format!("{i}"), Kinding::Natural);
            compare_value_at(gv, cv, egress, value_ty, &value_at)
                .map_err(|why| format!("[{i}] {why}"))?;
        }
        return Ok(());
    }
    let mut g: Vec<(Canon, &Value)> = Vec::with_capacity(golden.len());
    for (k, v) in golden {
        g.push((canon_golden_key(k)?, v));
    }
    let mut c: Vec<(Canon, &Value)> = Vec::with_capacity(cli.len());
    for entry in cli {
        let (key, value) = pair(entry, egress)?;
        c.push((canon_cli_key(key)?, value));
    }
    if g.len() != c.len() {
        return Err(format!("map size golden {} vs cli {}", g.len(), c.len()));
    }
    for (i, ((gk, gv), (ck, cv))) in g.iter().zip(c.iter()).enumerate() {
        if gk != ck {
            return Err(format!(
                "map key at emitted position {i}: golden {} vs cli {} — a map's \
                 entries are compared in EMITTED order, which is the key-comparator \
                 order both the dump and a reader of the same SSTable see (golden \
                 keys [{}], cli keys [{}])",
                brief(&gk.describe()),
                brief(&ck.describe()),
                keys_of(&g),
                keys_of(&c)
            ));
        }
        // A map VALUE is the cell value (`writeRawValue`), so it keeps its natural
        // JSON kind even when the key beside it was stringified.
        //
        // The PATH takes the key UNTRUNCATED: a declared gap is matched against it by
        // exact string (see [`gap::SkipPaths::declared`]), so truncating it here would
        // silently merge the paths of two long keys — DD1 again, one level down. Only
        // the message prefix is truncated.
        let key_text = gk.describe();
        let entry = at.index(&key_text, Kinding::Natural);
        compare_value_at(gv, cv, egress, value_ty, &entry)
            .map_err(|why| format!("[{}] {why}", brief(&key_text)))?;
    }
    Ok(())
}

/// The canonical keys of one side of a map, in emitted order, for the ordering
/// diagnostic above — a bare "golden X vs cli Y" at position 3 does not say
/// whether the entry is missing, extra or merely moved.
///
/// Each key is rendered through [`brief`] because a map key may be a 4 KiB blob and
/// this line lists EVERY key. That truncation is confined to this message: the keys
/// themselves are compared as `Canon` values (see [`compare_map`] and finding DD1).
fn keys_of(entries: &[(Canon, &Value)]) -> String {
    entries
        .iter()
        .map(|(k, _)| brief(&k.describe()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `compare_map` with a CONTAINER-typed key, and the three properties that had to
/// survive that (issue #3726).
#[cfg(test)]
#[path = "golden_value_compare_map_tests.rs"]
mod tests;
