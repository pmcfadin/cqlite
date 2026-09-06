//! Inverting the CSV rendering: one flat cell back into the golden's shape (#1491).
//!
//! Split out of `golden_csv_container.rs` under the campsite rule (CLAUDE.md, epic
//! #1135), which the container-map-key work pushed past the ~1500-line test-file
//! target. The seam is the module's two halves, which ask DIFFERENT questions:
//!
//!   * the parent decides whether a node CAN be read back at all — `node_refusal`,
//!     `decode_does_not_recover`, `golden_rendering`, and the spellings those rest
//!     on. That is a question about the GOLDEN and the format.
//!   * this file performs the read — `decode` and the grammar helpers under it. It
//!     runs only where the parent has already declined to refuse THIS NODE'S BODY;
//!     a `Reach::MapKeys` refusal (#3815) leaves the node splittable and suppresses
//!     the ambiguous KEYS alone, inside `decode_object`.
//!
//! No surface change: `decode`, `decode_at` and `Excluded` are re-exported by the
//! parent, so every call site is unchanged.

use super::*;

/// A predicate over a value path: `true` means the comparison excludes that path,
/// so the decoder must not require the CLI's text there to invert the grammar.
///
/// Kept as a bare closure type rather than a dependency on the comparator's
/// `SkipPaths`, so this module stays independent of it.
pub type Excluded<'a> = dyn Fn(&str) -> bool + 'a;

/// Decode `text` (one CSV field, or one member of one) into the shape `golden`
/// declares, with `ty` — the column's DECLARED CQL type — supplying the bracket
/// each container kind must be rendered with. A map/UDT decodes to the
/// `[{"key":…,"value":…}, …]` spelling the JSON egress uses, so the existing map
/// comparison applies unchanged.
pub fn decode(golden: &Value, text: &str, ty: &CqlType, kinding: Kinding) -> Result<Value, String> {
    decode_at(golden, text, ty, "", &|_| false, kinding)
}

/// [`decode`], but aware of the paths the comparison excludes.
///
/// `path` is the fully-qualified position of `text` in the row, spelled the same
/// way the comparator spells it (`col.field` for a named field, `col[i]` for a
/// positional member). At an EXCLUDED path the decode is ATTEMPTED and its result
/// used when it succeeds, falling back to the raw, UNDECODED text only when the
/// grammar does not invert there. That fallback is what keeps one un-invertible
/// member from failing a whole cell nobody compares — which is what forced the
/// `udt_nested` exclusion to be whole-column (issue #1491 review finding F5).
///
/// Attempting it rather than short-circuiting matters for the STALENESS side
/// (finding L1): an exclusion is applied only while it suppresses a real
/// divergence, and an unconditional raw-text answer here would keep the excluded
/// position diverging (an object against a string) even after CQLite renders it
/// correctly — so the gap could never retire itself.
///
/// The refusal scan is deliberately NOT exclusion-aware: it is decided from the
/// golden alone, and refusing a node is a conservative, counted, NAMED outcome in
/// the census, never a silent pass.
pub fn decode_at(
    golden: &Value,
    text: &str,
    ty: &CqlType,
    path: &str,
    excluded: &Excluded<'_>,
    kinding: Kinding,
) -> Result<Value, String> {
    if excluded(path) {
        return Ok(decode_shape(golden, text, ty, path, excluded, kinding)
            .unwrap_or_else(|_| Value::String(text.to_string())));
    }
    decode_shape(golden, text, ty, path, excluded, kinding)
}

/// The decode itself, with no exclusion check of its own at this level — that
/// belongs to [`decode_at`], so this can be run for an excluded path too. Nested
/// members still go through [`decode_at`], so a deeper exclusion applies normally.
fn decode_shape(
    golden: &Value,
    text: &str,
    ty: &CqlType,
    path: &str,
    excluded: &Excluded<'_>,
    kinding: Kinding,
) -> Result<Value, String> {
    // A node the GOLDEN's own content makes unsplittable is not split: its FRAME
    // is still required (that is `strip`, i.e. property 2 of
    // `decidable_despite_node_refusal`, applied at every depth) and its un-split
    // BODY is handed on for the count bounds. Interpreting the body here is what
    // would produce a member count no reading of the rendering supports — and
    // deciding the refusal for the whole CELL instead is what let an ambiguous
    // NESTED member suppress its unambiguous siblings and the outer structure
    // (review finding P2).
    //
    // The comparator asks `node_refusal` at the same node, on the same golden and
    // the same declared type, so it expects exactly what is left here.
    match node_refusal_reach(golden, Some(ty), kinding) {
        Some((Reach::Body, _)) => return Ok(Value::String(strip(text, ty)?.to_string())),
        // A MAP node whose KEYS ALONE are refused is SPLIT and decoded (issue
        // #3815): the entry boundaries and the emitted order are recoverable — that
        // is what `Reach::MapKeys` promises, and the checks that decide it are the
        // ones `decode_does_not_recover` runs BEFORE reaching the key-scoped cause.
        // Returning the un-split body here instead is what cost such a cell its
        // entry COUNT, its pair SHAPE and every VALUE in it. `decode_object`
        // suppresses the ambiguous KEYS and nothing else.
        Some((Reach::MapKeys, _)) | None => {}
    }
    // The declared TYPE decides the structure — including which bracket is
    // required — and the golden decides the member shapes underneath it. When the
    // two disagree the child is decoded against `null`, and the comparison is what
    // reports the shape divergence.
    match ty {
        CqlType::List(element) | CqlType::Set(element) => decode_sequence(
            golden,
            text,
            ty,
            &|_| Some(element),
            path,
            excluded,
            member_kinding(ty, kinding),
        ),
        CqlType::Tuple(items) => decode_sequence(
            golden,
            text,
            ty,
            &|i| items.get(i),
            path,
            excluded,
            member_kinding(ty, kinding),
        ),
        CqlType::Map(_, value_ty) => {
            decode_object(golden, text, ty, &|_| Some(value_ty), path, excluded)
        }
        CqlType::Udt(udt) => decode_object(
            golden,
            text,
            ty,
            &|field| {
                udt.fields
                    .iter()
                    .find(|(name, _)| name == field)
                    .map(|(_, t)| t)
            },
            path,
            excluded,
        ),
        // NULL-TOKEN: the golden's own type resolves the `null` token.
        _ => match golden {
            Value::Null if text == "null" => Ok(Value::Null),
            _ => Ok(Value::String(text.to_string())),
        },
    }
}

/// A list / set / tuple: one bracket pair fixed by `ty`, `, `-separated members,
/// each decoded under the element type `element_ty` gives for its position.
///
/// `element_ty` answers `None` only for a member BEYOND a tuple's declared arity,
/// which has no declared type; such a member is kept as raw text so the
/// comparator reports the arity divergence rather than the decoder swallowing it.
fn decode_sequence<'t>(
    golden: &Value,
    text: &str,
    ty: &'t CqlType,
    element_ty: &dyn Fn(usize) -> Option<&'t CqlType>,
    path: &str,
    excluded: &Excluded<'_>,
    element_kinding: Kinding,
) -> Result<Value, String> {
    let parts = members(text, ty)?;
    let items = golden.as_array();
    let mut out = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        // A member the golden does not have is decoded against `null`; the
        // length mismatch is what the comparison then reports.
        let child_golden = items.and_then(|g| g.get(i)).unwrap_or(&Value::Null);
        out.push(match element_ty(i) {
            Some(et) => decode_at(
                child_golden,
                part,
                et,
                &format!("{path}[{i}]"),
                excluded,
                element_kinding,
            )?,
            None => Value::String((*part).to_string()),
        });
    }
    Ok(Value::Array(out))
}

/// A map or UDT: `{…}`, `, `-separated `key: value` entries. `value_ty` answers
/// with the declared type of the value under a given key — `None` for a UDT field
/// the `CREATE TYPE` does not declare, whose value is therefore left as raw text
/// for the comparator to reject by name.
///
/// # A MAP's key is a VALUE and is decoded under its declared type (issue #3726)
///
/// A UDT entry's key is a FIELD NAME and stays verbatim. A map entry's key is a
/// value, so when the DDL declares a CONTAINER key type the key text is decoded
/// recursively like any other position — without that, CSV handed
/// `compare::compare_map` a flat string where the declared type says a container,
/// and the CSV half of a container-keyed map could not be compared at all.
///
/// # WHICH golden entry is this one, and why the answer is a RENDERING
///
/// The golden's own key is looked up by the text [`entry_key_rendering`] renders it
/// as — the same text [`decode_does_not_recover`] requires the decoder to recover
/// from the golden's own rendering — so the decoder and the refusal agree about
/// which entry is which. Matching the CLI's raw key text against the golden's
/// object key directly (the previous rule) held only while the two spellings
/// coincided, which is exactly what a container key does not do (nor, latently, a
/// `blob` key, whose golden `getString` text is the BARE hex while the CSV egress
/// renders `0x…`).
fn decode_object<'t>(
    golden: &Value,
    text: &str,
    ty: &'t CqlType,
    value_ty: &dyn Fn(&str) -> Option<&'t CqlType>,
    path: &str,
    excluded: &Excluded<'_>,
) -> Result<Value, String> {
    let parts = members(text, ty)?;
    let fields = golden.as_object();
    let container_key_ty = match ty {
        CqlType::Map(key_ty, _) if container::is_container_type(key_ty) => Some(&**key_ty),
        _ => None,
    };
    // WHICH of this node's entry KEYS are refused (issue #3815). Computed once, for
    // the same reason the renderings below are, and asked of the SAME function the
    // comparator asks — so what the decoder leaves at a key is exactly what
    // `compare::compare_map` expects to find there.
    let key_refused: Vec<Option<String>> = match (fields, map_key_ty(ty)) {
        (Some(g), Some(key_ty)) => map_key_refusals(g, key_ty),
        _ => Vec::new(),
    };
    // A key-scoped refusal makes the by-RENDERING lookup below multi-valued — that
    // ambiguity IS the refusal — so the golden entry is resolved POSITIONALLY
    // instead. Not a fallback but the rule the comparison runs on: a map's entries
    // are compared in EMITTED order, which both sides preserve, and
    // `compare::compare_map` pairs this node's entries by exactly that.
    let keys_refused_here = key_refused.iter().any(Option::is_some);
    // The golden's keys AS RENDERED, computed ONCE: a container key's rendering
    // parses a JSON document, and doing that per (entry x golden key) would be
    // quadratic in parses rather than in string compares. A golden key that does not
    // render is absent from this list, so it matches no entry and the comparison
    // reports the difference.
    let rendered_golden_keys: Vec<(String, &String)> = fields
        .map(|g| {
            g.keys()
                .filter_map(|key| entry_key_rendering(ty, key).map(|rendered| (rendered, key)))
                .collect()
        })
        .unwrap_or_default();
    // …and the golden's keys AS CANONICAL VALUES, for the fallback below. Same
    // once-only reason. A key that does not canonicalize is absent, so it matches
    // nothing.
    let canonical_golden_keys: Vec<(Canon, Value, &String)> = match (fields, map_key_ty(ty)) {
        (Some(g), Some(key_ty)) => g
            .keys()
            .filter_map(|key| {
                canonical_golden_key(key, key_ty).map(|(canon, value)| (canon, value, key))
            })
            .collect(),
        _ => Vec::new(),
    };
    let mut out = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        let (key, value) = entry_cut(part)?;
        // A UDT field step, spelled `parent.field` as the comparator spells it. A
        // MAP key reaches the same branch (CSV cannot tell the two apart), but a
        // dotted skip path through a map is rejected when the case is validated
        // against the DDL, so no exclusion can ever name one.
        let child = if path.is_empty() {
            key.to_string()
        } else {
            format!("{path}.{key}")
        };
        // WHICH GOLDEN ENTRY DOES THIS CSV ENTRY MEAN? Two matches, and they are DUALS
        // rather than two notions of the same thing (roborev, issue #3726):
        //
        //   * by RENDERED TEXT — cheap, and exact where both sides spell the value the
        //     same way, which is the overwhelmingly common case. It is unambiguous by
        //     PRECONDITION: colliding renderings are a KEY-SCOPED refusal
        //     (`map_key_refusals`), and this branch is not taken at a node that has one
        //     — `keys_refused_here` resolves the golden entry positionally instead — so
        //     this can never be the "first of several identical spellings" it would
        //     otherwise be. Until #3815 that precondition was enforced one level
        //     coarser, by refusing the whole node, which cost the node's entry count,
        //     pair shape and values.
        //   * by CANONICAL VALUE — for keys the two sides legitimately SPELL differently.
        //     `entry_key_rendering` translates the spellings this lane knows
        //     (`stringified_csv_text` handles `blob`) and deliberately leaves the rest
        //     alone, so a `timestamp` golden key reads `2024-01-01T00:00:00Z` where the
        //     CSV cell reads `2024-01-01 00:00:00+0000`. `canon_timestamp` accepts BOTH
        //     separators — that is this lane stating in its own source that the two
        //     spellings denote one value — so the comparison pairs them while a text
        //     match cannot. Without this the key got NO guide, and a container key
        //     holding a `null` member then decoded that token as the TEXT `"null"`
        //     instead of `Null`, reporting CORRECT egress as a divergence.
        //
        // The two answer OPPOSITE questions and neither subsumes the other: the refusal
        // above covers DIFFERENT values that share one spelling, this covers ONE value
        // spelled two ways. An AMBIGUOUS canonical match (two golden keys of equal
        // canonical value, e.g. a numeric `1` and `1.0`) selects NOTHING — a guide is
        // never guessed at.
        let golden_key = if keys_refused_here {
            fields.and_then(|g| g.keys().nth(index))
        } else {
            rendered_golden_keys
                .iter()
                .find(|(rendered, _)| rendered == key)
                .map(|(_, golden_key)| *golden_key)
                .or_else(|| {
                    let key_ty = map_key_ty(ty)?;
                    let mut hit = None;
                    for (canon_golden, guide, golden_key) in &canonical_golden_keys {
                        // ASK THE QUESTION THE OTHER WAY ROUND. Canonicalizing the CSV text
                        // on its own and comparing cannot work, and the reason is a
                        // circularity worth stating: reading that text needs the very guide
                        // we are trying to choose. With no guide the token `null` reads as
                        // `Null`, so a golden slot holding the TEXT `"null"` never matches
                        // its own entry. (A first attempt did exactly that and could not fix
                        // this case.)
                        //
                        // So each CANDIDATE is tried AS the guide: does this CSV text, read
                        // under candidate `g`, denote `g`? A candidate that answers yes is a
                        // consistent reading of the entry; one that answers no is not.
                        if canonical_cli_key(key, key_ty, guide, excluded).as_ref()
                            == Some(canon_golden)
                        {
                            if hit.is_some() {
                                return None;
                            }
                            hit = Some(*golden_key);
                        }
                    }
                    hit
                })
        };
        let mut entry = Map::new();
        entry.insert(
            "key".to_string(),
            match container_key_ty {
                // A REFUSED key leaves its stripped BODY, exactly as
                // [`decode_shape`] leaves any refused node — so the comparator
                // receives at this position the one thing it can still decide there,
                // the body's EMPTINESS, and never a key value that would have to be
                // GUESSED (issue #3815). Guessing is precisely what the ambiguity
                // forbids: two golden keys sharing one spelling make EITHER reading
                // self-consistent, so accepting one would report CORRECT egress as a
                // divergence for whichever entry guessed wrong (#1491 finding T1,
                // which #3726's refusal closed and this must not reopen).
                //
                // The FRAME is still required, at this node's own depth, by the same
                // `strip` and with its error PROPAGATED — a wrong bracket here is a
                // divergence, not an ambiguity, and `compare_map` cannot see it once
                // the frame is gone.
                Some(key_ty) if key_refused.get(index).is_some_and(Option::is_some) => {
                    Value::String(strip(key, key_ty)?.to_string())
                }
                Some(key_ty) => {
                    // The golden's own parsed key guides the decode where there is
                    // one; a key the golden does not have is decoded from the
                    // declared type alone and the comparison reports the difference.
                    let guide = golden_key
                        .and_then(|k| {
                            // Asking the same question as `entry_key_rendering`: is
                            // there a toJSONString document here to guide the decode?
                            // A `getString` key answers no and the decode falls back
                            // to the declared type alone.
                            container::golden_map_key_value(
                                k,
                                key_ty,
                                container::MapKeySpelling::ToJsonString,
                            )
                            .ok()
                        })
                        .unwrap_or(Value::Null);
                    // A key whose text does not invert the grammar is left as RAW
                    // TEXT rather than failing the whole cell — the same rule this
                    // module already applies to a member beyond a tuple's declared
                    // arity and to an undeclared UDT field. Nothing is swallowed:
                    // `compare::compare_map` canonicalizes that text under the
                    // declared container key type, which REFUSES a flat scalar and
                    // names the position.
                    //
                    // The ENTRY's path is reused for the key: the only thing that
                    // reads it is the exclusion predicate, and no exclusion can name
                    // a path inside a map (a dotted skip path through one is rejected
                    // when the case is validated against the DDL).
                    decode_at(&guide, key, key_ty, &child, excluded, Kinding::Natural)
                        .unwrap_or_else(|_| Value::String(key.to_string()))
                }
                None => Value::String(key.to_string()),
            },
        );
        // THE VALUE'S GUIDE, with a POSITIONAL fallback (roborev job 36).
        //
        // A MULTICELL container-keyed map resolves NO `golden_key` at all: the golden's
        // object key is `getString`'s cell-path text, which is not the declared type's
        // `toJSONString` document, so it renders to nothing and matches no entry. Every
        // value in such a map was therefore decoded against `Value::Null` — and that is
        // not inert, because `decode_shape`'s null-token arm reads the token `null` as
        // `Value::Null` only when the guide is null. A legitimate TEXT value spelled
        // `null` was decoded as an actual null and reported as a divergence it is not.
        //
        // Falling back to the i-th golden entry is not a guess: a map's entries are
        // compared in EMITTED ORDER — that is `compare::map::compare_map`'s pairing rule,
        // which both sides preserve — so the i-th CSV entry's value belongs to the i-th
        // golden entry by the same rule the comparison will use on it.
        let child_golden = golden_key
            .and_then(|k| fields.and_then(|g| g.get(k)))
            .or_else(|| fields.and_then(|g| g.values().nth(index)))
            .unwrap_or(&Value::Null);
        let decoded = match value_ty(key) {
            // A map VALUE / UDT field is a cell value: natural kind, as in
            // `golden_rendering`'s object arm and in the comparator.
            Some(vt) => decode_at(child_golden, value, vt, &child, excluded, Kinding::Natural)?,
            None => Value::String(value.to_string()),
        };
        entry.insert("value".to_string(), decoded);
        out.push(Value::Object(entry));
    }
    Ok(Value::Array(out))
}

/// Cut one map/UDT entry into its key and its value at the FIRST top-level `: `,
/// which is the only cut the grammar defines: a `: ` in a VALUE is ordinary text.
///
/// Factored out because [`decode_object`] and [`decode_does_not_recover`] must make
/// the IDENTICAL cut — the refusal asks whether this cut gives the golden's key
/// back, so a second spelling of it is a second notion of decodability, which is
/// the drift this lane's review history is made of.
pub(super) fn entry_cut(part: &str) -> Result<(&str, &str), String> {
    let cut = *scan(part, ": ")?.first().ok_or_else(|| {
        format!(
            "map/UDT entry {} has no top-level `: ` separator",
            brief(part)
        )
    })?;
    Ok((&part[..cut], &part[cut + 2..]))
}

/// Strip the bracket pair `ty` requires and split the body at every depth-zero
/// `, `. An empty body is zero members.
pub(super) fn members<'a>(text: &'a str, ty: &CqlType) -> Result<Vec<&'a str>, String> {
    let inner = strip(text, ty)?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    split_top_level(inner, ", ")
}

/// Remove the opening/closing bracket pair the DECLARED type requires. Strict in
/// both directions: a body that does not open with THAT bracket, or does not close
/// with its mate, is an error rather than a best-effort parse — so a `set`
/// rendered `[a, b]` fails instead of being read as a list.
pub(super) fn strip<'a>(text: &'a str, ty: &CqlType) -> Result<&'a str, String> {
    let (open, close) = brackets(ty).ok_or_else(|| {
        format!(
            "{} was decoded as a container but the schema declares the scalar type \
             `{}`",
            brief(text),
            ty.describe()
        )
    })?;
    let rest = text.strip_prefix(open).ok_or_else(|| {
        format!(
            "{} is not a `{}` rendering: the declared type requires an opening \
             `{open}` (`{open}…{close}`)",
            brief(text),
            ty.describe()
        )
    })?;
    rest.strip_suffix(close).ok_or_else(|| {
        format!(
            "{} opens with `{open}` but does not close with `{close}`",
            brief(text)
        )
    })
}

/// Split `body` at every depth-zero `sep`.
pub(super) fn split_top_level<'a>(body: &'a str, sep: &str) -> Result<Vec<&'a str>, String> {
    let cuts = scan(body, sep)?;
    let mut parts = Vec::with_capacity(cuts.len() + 1);
    let mut start = 0usize;
    for cut in cuts {
        parts.push(&body[start..cut]);
        start = cut + sep.len();
    }
    parts.push(&body[start..]);
    Ok(parts)
}

/// Byte offsets of every depth-zero, non-overlapping `sep` in `body`.
///
/// Iterates by `char_indices` so slicing stays on UTF-8 boundaries (member text
/// is arbitrary CQL `text`). Unbalanced brackets are an error — the rendering is
/// then not the grammar this decoder inverts, and silently tolerating it is how
/// a decoder starts absorbing writer defects.
pub(super) fn scan(body: &str, sep: &str) -> Result<Vec<usize>, String> {
    let mut cuts = Vec::new();
    let mut depth: i32 = 0;
    let mut consumed = 0usize;
    for (idx, ch) in body.char_indices() {
        match ch {
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err(format!(
                        "{} closes a bracket that never opened",
                        brief(body)
                    ));
                }
            }
            _ => {}
        }
        if depth == 0 && idx >= consumed && body[idx..].starts_with(sep) {
            cuts.push(idx);
            consumed = idx + sep.len();
        }
    }
    if depth != 0 {
        return Err(format!(
            "{} leaves {depth} bracket(s) unclosed",
            brief(body)
        ));
    }
    Ok(cuts)
}
