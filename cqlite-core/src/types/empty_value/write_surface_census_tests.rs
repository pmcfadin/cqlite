//! CRATE-WIDE CENSUS of every value-serializing entry point, and each one's
//! disposition toward the empty-buffer sentinel (issue #3805).
//!
//! # Why this exists — the SAME defect was found THREE times in three rounds
//!
//! | round | site | what it did with `Value::Empty` |
//! |---|---|---|
//! | roborev job 448 | `parser/types/mod.rs` (2 arms) | wrote a bare type byte its own reader rejects |
//! | roborev job 449 | `sstable/writer/data_writer/encoding.rs` | wrote zero bytes with no declared type in sight |
//! | roborev job 452 | `storage/serialization/types.rs` | wrote zero bytes as a general CELL VALUE |
//!
//! Each round patched one file. Nothing enumerated the OTHER files, so the next
//! round found the next one — and job 452's site had even been cited, in job
//! 448's own justification, as *the* surface that could legitimately write the
//! sentinel. A curated list that silently misses a site is exactly what produced
//! three rounds of this, so the subject set here is **DERIVED FROM SOURCE at test
//! time** and the committed table must match it EXACTLY, in both directions.
//!
//! # The invariant
//!
//! The sentinel's zero-byte form is legal at exactly ONE position in this crate —
//! a MULTICELL map's CELL PATH — because that is the only position supplying BOTH
//! halves of the admission: a DECLARED KEY TYPE to validate the tag against
//! ([`crate::types::EmptyValueType::check_admits`]) and a FRAMING in which a
//! zero-length buffer is expressible and MEANS "empty" (the enclosing collection's
//! unsigned-VInt length, `db/marshal/CollectionType.java:361-382` at
//! `cassandra-5.0.8`). Everywhere else, refuse rather than guess — refusing beats
//! writing bytes that read back as something else (no-heuristics, issue #28).
//!
//! So: **exactly one entry in this census may ADMIT the sentinel**, and that is
//! asserted, not assumed.
//!
//! # What each check measures
//!
//! * [`the_derived_census_matches_the_committed_dispositions`] — the derived set
//!   and the table agree both ways, so a NEW value serializer FAILs until it is
//!   given a disposition, and a DELETED one FAILs a stale row.
//! * [`exactly_one_census_entry_admits_the_sentinel`] — the invariant above.
//! * [`every_disposition_is_structurally_true_of_its_function`] — a
//!   `RefusesNoSentinelArm` function's body must contain NO `Value::Empty`
//!   pattern, and the two dispositions that DO may only be reached by a row that
//!   is also behaviourally pinned. This is the mechanization: adding an
//!   admitting arm anywhere reds this file rather than waiting for round four.
//! * [`every_sentinel_arm_is_behaviourally_pinned`] + the behavioural tests
//!   below — a label is a claim about runtime, so each function carrying a
//!   `Value::Empty` arm is CALLED and its answer asserted.
//!
//! # Declared NON-EXHAUSTIVENESS (read this before trusting the count)
//!
//! The derivation recognises a value serializer by SIGNATURE SHAPE. It therefore
//! does NOT reach:
//!
//! * a serializer taking `&[Value]` rather than `&Value` — e.g.
//!   `bti::parser::encoding::encode_clustering_bound_oss50` and
//!   `bti::encoder::encode_composite_key`. Both delegate to a per-component
//!   function that IS in the census, so the sentinel's disposition is decided
//!   there, but the shape itself is unrecognised;
//! * a serializer that writes bytes through anything other than a
//!   `Result<Vec<u8>>` return, a `&mut Vec<u8>` parameter, or `&mut self` +
//!   `Result<()>` (e.g. an `impl Write` sink);
//! * anything in a file under a `tests/` directory or named `*_tests.rs` — test
//!   fixtures may legitimately construct sentinel bytes. Note the asymmetry: an
//!   inline `#[cfg(test)] mod tests` inside a production file IS scanned, and
//!   measured to add nothing today.
//!
//! Widening the shape is a one-line change to [`produces_bytes`]; the table then
//! FAILs until the new rows get dispositions, which is the intended direction.

use crate::types::{EmptyValueType, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

// ───────────────────────────────────────────────────────────────────────────
// THE COMMITTED DISPOSITIONS
// ───────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    /// The ONE position that may write the sentinel: a multicell map's cell
    /// path, tag validated against the declared key type.
    AdmitsMapCellPath,
    /// Carries its OWN `Value::Empty` arm, which REFUSES. Must be behaviourally
    /// pinned: a refusing arm is a runtime claim.
    RefusesExplicit,
    /// Carries NO `Value::Empty` arm at all, so the sentinel can only reach this
    /// function's catch-all (an error) or a delegate that refuses. Verified
    /// STRUCTURALLY — the absence of special-casing is what is asserted.
    RefusesNoSentinelArm,
}

use Disposition::{AdmitsMapCellPath, RefusesExplicit, RefusesNoSentinelArm};

/// One row per DERIVED value-serializing function, keyed
/// `(path relative to cqlite-core/src, function name)`.
///
/// This table is not the subject set — [`census`] is. This is the DISPOSITION of
/// each derived subject, and the two must match exactly.
const DISPOSITIONS: &[(&str, &str, Disposition)] = &[
    // ── the legacy tagged CQL value format (roborev job 448) ──
    (
        "parser/types/mod.rs",
        "serialize_cql_value",
        RefusesExplicit,
    ),
    (
        "parser/types/mod.rs",
        "serialize_value_without_type_prefix",
        RefusesExplicit,
    ),
    // ── the type-aware writer (roborev job 452 — THIS round) ──
    (
        "storage/serialization/types.rs",
        "serialize_value",
        RefusesExplicit,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_typed_value",
        RefusesExplicit,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_primitive",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_text",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_blob",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_temporal",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_numeric",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_inet",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_list",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_set",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_map",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_tuple",
        RefusesNoSentinelArm,
    ),
    (
        "storage/serialization/types.rs",
        "serialize_udt",
        RefusesNoSentinelArm,
    ),
    // ── the SSTable writer's type-blind encoder (roborev job 449) ──
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "serialize_map_cell_path_key_into",
        AdmitsMapCellPath,
    ),
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "serialize_value_into",
        RefusesExplicit,
    ),
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "serialize_value",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "serialize_collection_element",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "serialize_collection_element_into",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "write_cell_value_into",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "serialize_value_for_clustering",
        RefusesNoSentinelArm,
    ),
    // ── the writer's cell/complex-column drivers (they delegate) ──
    (
        "storage/sstable/writer/data_writer/cells.rs",
        "write_cell",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/cells.rs",
        "write_cell_with_ttl",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/cells.rs",
        "write_cell_with_row_ttl",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/cells.rs",
        "write_cell_explicit_ts",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/complex.rs",
        "write_complex_column",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/complex.rs",
        "write_list_complex_cells",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/complex.rs",
        "write_set_complex_cells",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/complex.rs",
        "write_map_complex_cells",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/writer/data_writer/complex.rs",
        "write_udt_complex_cells",
        RefusesNoSentinelArm,
    ),
    // ── BTI byte-comparable keys ──
    (
        "storage/sstable/bti/encoder.rs",
        "encode_value_to_buffer_with_depth",
        RefusesExplicit,
    ),
    (
        "storage/sstable/bti/encoder.rs",
        "encode_value_to_buffer",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/bti/encoder.rs",
        "encode_value",
        RefusesNoSentinelArm,
    ),
    (
        "storage/sstable/bti/parser/encoding.rs",
        "encode_clustering_component_oss50",
        RefusesNoSentinelArm,
    ),
    // ── partition-key encoding ──
    (
        "storage/partition_key_codec.rs",
        "encode_single_component_key_typed",
        RefusesNoSentinelArm,
    ),
    (
        "storage/partition_key_codec.rs",
        "serialize_value_bytes",
        RefusesNoSentinelArm,
    ),
    (
        "storage/write_engine/mutation.rs",
        "serialize_value",
        RefusesNoSentinelArm,
    ),
    (
        "storage/write_engine/mutation.rs",
        "serialize_value_bytes",
        RefusesNoSentinelArm,
    ),
    (
        "query/executor.rs",
        "value_to_raw_pk_bytes",
        RefusesNoSentinelArm,
    ),
];

/// Every row whose disposition permits a `Value::Empty` arm must be CALLED by a
/// test in this file, because a label about refusing (or admitting) is a claim
/// about runtime and a structural check cannot make it.
///
/// The third field names the test that does it, and how — a private function is
/// reached through the public entry point named there.
const BEHAVIOURALLY_PINNED: &[(&str, &str, &str)] = &[
    (
        "parser/types/mod.rs",
        "serialize_cql_value",
        "the_tagged_format_refuses_the_sentinel (direct)",
    ),
    (
        "parser/types/mod.rs",
        "serialize_value_without_type_prefix",
        "the_tagged_format_refuses_the_sentinel (via serialize_cql_value on a list)",
    ),
    (
        "storage/serialization/types.rs",
        "serialize_value",
        "the_type_aware_writer_refuses_the_sentinel (direct)",
    ),
    (
        "storage/serialization/types.rs",
        "serialize_typed_value",
        "the_type_aware_writer_refuses_the_sentinel (via serialize_value on list<int>)",
    ),
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "serialize_map_cell_path_key_into",
        "the_one_admitting_position_admits_and_writes_zero_bytes (direct)",
    ),
    (
        "storage/sstable/writer/data_writer/encoding.rs",
        "serialize_value_into",
        "the_type_blind_writer_refuses_the_sentinel (direct)",
    ),
    (
        "storage/sstable/bti/encoder.rs",
        "encode_value_to_buffer_with_depth",
        "the_bti_key_encoder_refuses_the_sentinel (via encode_value)",
    ),
];

/// A derivation that suddenly matches (almost) nothing is a BROKEN derivation,
/// not a clean crate — the vacuous-pass shape. Measured 40 on the fix commit;
/// the floor is deliberately below that so ordinary refactoring does not red it,
/// and far above zero.
const DERIVED_FLOOR: usize = 30;

// ───────────────────────────────────────────────────────────────────────────
// THE DERIVATION
// ───────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct SerializerFn {
    file: String,
    name: String,
    body: String,
}

/// Does this signature PRODUCE BYTES from the `&Value` it consumes?
///
/// Three recognised shapes, all measured against the crate as it stands:
/// an owned byte return, a caller-supplied byte sink, or a method filling its
/// own buffer. See the module's declared non-exhaustiveness.
fn produces_bytes(sig: &str) -> bool {
    sig.contains("Result<Vec<u8>>")
        || sig.contains("&mut Vec<u8>")
        || (sig.contains("&mut self") && sig.contains("Result<()>"))
}

/// Blank out comments, string literals (raw and byte forms included) and char
/// literals, replacing each byte with a space and preserving newlines.
///
/// Everything downstream — function discovery, signature extraction and brace
/// matching — runs on this stripped text, which is what makes the brace matching
/// exact (a `'{'` char literal or a `"{"` in a diagnostic cannot unbalance it)
/// and what makes a `Value::Empty` mentioned in a DOC COMMENT or an error
/// MESSAGE correctly not count as a pattern arm.
fn strip_comments_and_literals(src: &str) -> String {
    let b: Vec<char> = src.chars().collect();
    let mut out: Vec<char> = b.clone();
    let n = b.len();
    let mut i = 0usize;
    // Blank `[from, to)` but keep newlines so line structure survives.
    let blank = |out: &mut Vec<char>, from: usize, to: usize| {
        for k in from..to.min(n) {
            if out[k] != '\n' {
                out[k] = ' ';
            }
        }
    };
    while i < n {
        let c = b[i];
        if c == '/' && i + 1 < n && b[i + 1] == '/' {
            let mut j = i;
            while j < n && b[j] != '\n' {
                j += 1;
            }
            blank(&mut out, i, j);
            i = j;
        } else if c == '/' && i + 1 < n && b[i + 1] == '*' {
            let mut depth = 1usize;
            let mut j = i + 2;
            while j < n && depth > 0 {
                if b[j] == '/' && j + 1 < n && b[j + 1] == '*' {
                    depth += 1;
                    j += 2;
                } else if b[j] == '*' && j + 1 < n && b[j + 1] == '/' {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            blank(&mut out, i, j);
            i = j;
        } else if let Some((start, hashes)) = raw_string_open(&b, i) {
            // r"…" / r#"…"# / br##"…"##
            let mut j = start;
            let mut end = n;
            while j < n {
                if b[j] == '"' {
                    let mut ok = true;
                    for h in 0..hashes {
                        if j + 1 + h >= n || b[j + 1 + h] != '#' {
                            ok = false;
                            break;
                        }
                    }
                    if ok {
                        end = j + 1 + hashes;
                        break;
                    }
                }
                j += 1;
            }
            blank(&mut out, i, end);
            i = end;
        } else if c == '"' {
            let mut j = i + 1;
            while j < n {
                if b[j] == '\\' {
                    j += 2;
                    continue;
                }
                if b[j] == '"' {
                    j += 1;
                    break;
                }
                j += 1;
            }
            blank(&mut out, i, j);
            i = j;
        } else if c == '\'' {
            // A char literal, or a lifetime. Only the former is blanked.
            if i + 2 < n && b[i + 1] == '\\' {
                let mut j = i + 2;
                while j < n && b[j] != '\'' {
                    j += 1;
                }
                blank(&mut out, i, (j + 1).min(n));
                i = (j + 1).min(n);
            } else if i + 2 < n && b[i + 2] == '\'' {
                blank(&mut out, i, i + 3);
                i += 3;
            } else {
                i += 1; // lifetime
            }
        } else {
            i += 1;
        }
    }
    out.into_iter().collect()
}

/// Recognise a raw-string opener at `i`, returning (index just past the opening
/// quote, number of `#`).
fn raw_string_open(b: &[char], i: usize) -> Option<(usize, usize)> {
    let mut j = i;
    if b.get(j) == Some(&'b') {
        j += 1;
    }
    if b.get(j) != Some(&'r') {
        return None;
    }
    j += 1;
    let hash_start = j;
    while b.get(j) == Some(&'#') {
        j += 1;
    }
    if b.get(j) != Some(&'"') {
        return None;
    }
    Some((j + 1, j - hash_start))
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("census cannot read {}: {e}", dir.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|e| panic!("census cannot read an entry of {}: {e}", dir.display()))
                .path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// DERIVE the subject set from committed source. A failure to read or parse is a
/// PANIC naming the cause, never a smaller set — a derivation that degrades to
/// "nothing found" would excuse every serializer in the crate.
fn census() -> Vec<SerializerFn> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    assert!(
        root.is_dir(),
        "census subject root {} is not a directory",
        root.display()
    );
    let mut out = Vec::new();
    for path in rust_sources(&root) {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or_else(|e| panic!("{} is not under {}: {e}", path.display(), root.display()))
            .to_string_lossy()
            .replace('\\', "/");
        // Test files may legitimately construct sentinel bytes (declared gap).
        if rel.contains("/tests/") || rel.starts_with("tests/") || rel.ends_with("_tests.rs") {
            continue;
        }
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("census cannot read {}: {e}", path.display()));
        for (name, body) in serializers_in(&raw) {
            out.push(SerializerFn {
                file: rel.clone(),
                name,
                body,
            });
        }
    }
    out
}

/// Every value-serializing function in ONE source text, as (name, stripped
/// body). Factored out of [`census`] so the scanner itself is self-tested
/// (`the_scanner_reads_pattern_arms_and_not_prose`) rather than trusted.
fn serializers_in(raw: &str) -> Vec<(String, String)> {
    let src: Vec<char> = strip_comments_and_literals(raw).chars().collect();
    let mut out = Vec::new();
    for (start, name) in fn_declarations(&src) {
        let Some(open) = src[start..].iter().position(|c| *c == '{' || *c == ';') else {
            continue;
        };
        let end = start + open;
        let sig: String = src[start..end].iter().collect();
        if !sig.contains("&Value") || !produces_bytes(&sig) {
            continue;
        }
        let body = if src[end] == '{' {
            brace_matched(&src, end)
        } else {
            String::new()
        };
        out.push((name, body));
    }
    out
}

/// Every `fn NAME` in stripped text, as (offset of `fn`, name).
fn fn_declarations(src: &[char]) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let n = src.len();
    let mut i = 0usize;
    while i + 2 < n {
        let is_word_start = i == 0 || !(src[i - 1].is_alphanumeric() || src[i - 1] == '_');
        if is_word_start && src[i] == 'f' && src[i + 1] == 'n' && src[i + 2].is_whitespace() {
            let mut j = i + 2;
            while j < n && src[j].is_whitespace() {
                j += 1;
            }
            let name_start = j;
            while j < n && (src[j].is_alphanumeric() || src[j] == '_') {
                j += 1;
            }
            if j > name_start {
                let name: String = src[name_start..j].iter().collect();
                // Skip generics, then require a parameter list.
                let mut k = j;
                while k < n && src[k].is_whitespace() {
                    k += 1;
                }
                if src.get(k) == Some(&'<') {
                    let mut depth = 0i32;
                    while k < n {
                        if src[k] == '<' {
                            depth += 1;
                        } else if src[k] == '>' {
                            depth -= 1;
                            if depth == 0 {
                                k += 1;
                                break;
                            }
                        }
                        k += 1;
                    }
                    while k < n && src[k].is_whitespace() {
                        k += 1;
                    }
                }
                if src.get(k) == Some(&'(') {
                    out.push((i, name));
                }
            }
            i = j.max(i + 2);
        } else {
            i += 1;
        }
    }
    out
}

/// The `{ … }` block starting at `open`, brace-matched over stripped text.
fn brace_matched(src: &[char], open: usize) -> String {
    let mut depth = 0i32;
    for (offset, c) in src[open..].iter().enumerate() {
        if *c == '{' {
            depth += 1;
        } else if *c == '}' {
            depth -= 1;
            if depth == 0 {
                return src[open..=open + offset].iter().collect();
            }
        }
    }
    panic!("census could not brace-match a function body at offset {open}");
}

fn key_set<T: Copy>(rows: &[(&'static str, &'static str, T)]) -> BTreeSet<(String, String)> {
    rows.iter()
        .map(|(f, n, _)| ((*f).to_string(), (*n).to_string()))
        .collect()
}

// ───────────────────────────────────────────────────────────────────────────
// (1) THE SUBJECT SET IS DERIVED, AND THE TABLE MUST MATCH IT EXACTLY
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn the_derived_census_matches_the_committed_dispositions() {
    let derived = census();
    assert!(
        derived.len() >= DERIVED_FLOOR,
        "the derivation matched only {} value serializers (floor {DERIVED_FLOOR}) — that is a \
         BROKEN derivation, not a clean crate; fix `produces_bytes`/`fn_declarations` rather \
         than lowering the floor",
        derived.len()
    );

    let derived_keys: BTreeSet<(String, String)> = derived
        .iter()
        .map(|f| (f.file.clone(), f.name.clone()))
        .collect();
    let declared = key_set(DISPOSITIONS);

    // PRINT the census on every run: a list that is never shown is a list nobody
    // re-checks, and this one is DECLARED non-exhaustive (see the module doc).
    println!(
        "empty-sentinel write-surface census: {} DERIVED value-serializing functions \
         (NON-EXHAUSTIVE by signature shape — see module doc)",
        derived_keys.len()
    );
    for f in &derived {
        println!("  {} :: {}", f.file, f.name);
    }

    let missing: Vec<_> = derived_keys.difference(&declared).collect();
    assert!(
        missing.is_empty(),
        "value-serializing function(s) with NO committed disposition toward the empty-buffer \
         sentinel: {missing:?}\n\
         Add a row to DISPOSITIONS. If the new function must never see the sentinel, that is \
         `RefusesNoSentinelArm` and it must carry no `Value::Empty` arm; if it refuses \
         explicitly it must also be behaviourally pinned; and if it ADMITS, note that exactly \
         ONE entry in this crate may (issue #3805, roborev jobs 448/449/452)."
    );
    let stale: Vec<_> = declared.difference(&derived_keys).collect();
    assert!(
        stale.is_empty(),
        "DISPOSITIONS row(s) naming a function the derivation no longer finds: {stale:?} — \
         either it was renamed/removed (drop the row) or the derivation broke (fix it)"
    );
    assert_eq!(
        derived_keys.len(),
        DISPOSITIONS.len(),
        "DISPOSITIONS holds a duplicate row"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// (2) EXACTLY ONE POSITION MAY ADMIT THE SENTINEL
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn exactly_one_census_entry_admits_the_sentinel() {
    let admitting: Vec<_> = DISPOSITIONS
        .iter()
        .filter(|(_, _, d)| *d == AdmitsMapCellPath)
        .map(|(f, n, _)| (*f, *n))
        .collect();
    assert_eq!(
        admitting,
        vec![(
            "storage/sstable/writer/data_writer/encoding.rs",
            "serialize_map_cell_path_key_into"
        )],
        "the sentinel's zero-byte form is legal at exactly ONE position — a multicell map's \
         cell path, the only one supplying BOTH a declared key type and a framing in which a \
         zero-length buffer means an empty key. Admitting it anywhere else is roborev jobs \
         448/449/452 happening a fourth time."
    );
}

// ───────────────────────────────────────────────────────────────────────────
// (3) EACH DISPOSITION IS STRUCTURALLY TRUE OF ITS FUNCTION
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn every_disposition_is_structurally_true_of_its_function() {
    let derived = census();
    let pinned = key_set(BEHAVIOURALLY_PINNED);
    for (file, name, disposition) in DISPOSITIONS {
        let f = derived
            .iter()
            .find(|f| f.file == *file && f.name == *name)
            .unwrap_or_else(|| {
                panic!("{file} :: {name} is in DISPOSITIONS but not in the derived census")
            });
        // The body is stripped of comments and string literals, so this counts
        // PATTERN ARMS and constructions — never a mention in a doc comment or a
        // diagnostic message.
        let has_arm = f.body.contains("Value::Empty");
        match disposition {
            RefusesNoSentinelArm => assert!(
                !has_arm,
                "{file} :: {name} is declared `RefusesNoSentinelArm` but its body PATTERN-MATCHES \
                 or CONSTRUCTS `Value::Empty`. Either it now handles the sentinel — in which case \
                 relabel it (`RefusesExplicit`, or `AdmitsMapCellPath` if it really is the map \
                 cell path) AND pin it behaviourally — or remove the arm."
            ),
            RefusesExplicit | AdmitsMapCellPath => {
                assert!(
                    has_arm,
                    "{file} :: {name} is declared `{disposition:?}`, which claims its own \
                     `Value::Empty` arm, but its body has none — relabel it \
                     `RefusesNoSentinelArm`"
                );
                assert!(
                    pinned.contains(&((*file).to_string(), (*name).to_string())),
                    "{file} :: {name} carries a `Value::Empty` arm, so its disposition is a claim \
                     about RUNTIME that a source scan cannot make — add it to \
                     BEHAVIOURALLY_PINNED and a test in this file that CALLS it"
                );
            }
        }
    }
}

#[test]
fn every_sentinel_arm_is_behaviourally_pinned() {
    let declared = key_set(DISPOSITIONS);
    for (file, name, how) in BEHAVIOURALLY_PINNED {
        assert!(
            declared.contains(&((*file).to_string(), (*name).to_string())),
            "BEHAVIOURALLY_PINNED names {file} :: {name} ({how}), which is not in DISPOSITIONS"
        );
        let disposition = DISPOSITIONS
            .iter()
            .find(|(f, n, _)| f == file && n == name)
            .map(|(_, _, d)| *d)
            .expect("checked above");
        assert_ne!(
            disposition, RefusesNoSentinelArm,
            "{file} :: {name} is pinned behaviourally but declared \
             `RefusesNoSentinelArm`; a no-arm function's disposition is the STRUCTURAL claim, so \
             either give it a sentinel arm and the matching label, or drop the pin"
        );
        assert!(
            !how.trim().is_empty(),
            "{file} :: {name} must say HOW it is pinned"
        );
    }
}

// ───────────────────────────────────────────────────────────────────────────
// (4) THE BEHAVIOURAL PINS — a label about runtime, measured at runtime
// ───────────────────────────────────────────────────────────────────────────

/// Every family the tag admits, so a refusal that holds for `int` and not
/// `varint` is not mistaken for a refusal.
///
/// `EmptyValueType` is not enumerable, so this list is CURATED with a
/// compile-time backstop: [`tag_is_accounted_for`] matches it with NO wildcard,
/// so adding a variant fails to COMPILE here until it is handled. DECLARED
/// residual — extending that match without extending this list is the one move
/// the backstop does not catch.
fn every_tag() -> Vec<EmptyValueType> {
    let all = vec![
        EmptyValueType::Boolean,
        EmptyValueType::Int,
        EmptyValueType::BigInt,
        EmptyValueType::Counter,
        EmptyValueType::Float,
        EmptyValueType::Double,
        EmptyValueType::Timestamp,
        EmptyValueType::Uuid,
        EmptyValueType::TimeUuid,
        EmptyValueType::Inet,
        EmptyValueType::Decimal,
        EmptyValueType::Varint,
    ];
    for tag in &all {
        tag_is_accounted_for(*tag);
    }
    all
}

/// The compile-time backstop for [`every_tag`]: no wildcard arm, so a new
/// `EmptyValueType` variant breaks the BUILD of this census rather than
/// silently escaping every behavioural pin in it.
fn tag_is_accounted_for(tag: EmptyValueType) {
    match tag {
        EmptyValueType::Boolean
        | EmptyValueType::Int
        | EmptyValueType::BigInt
        | EmptyValueType::Counter
        | EmptyValueType::Float
        | EmptyValueType::Double
        | EmptyValueType::Timestamp
        | EmptyValueType::Uuid
        | EmptyValueType::TimeUuid
        | EmptyValueType::Inet
        | EmptyValueType::Decimal
        | EmptyValueType::Varint => {}
    }
}

/// PIN for `serialize_map_cell_path_key_into` — the ONE admitting position:
/// admits a matching tag and writes exactly ZERO bytes.
#[test]
fn the_one_admitting_position_admits_and_writes_zero_bytes() {
    use crate::storage::sstable::writer::data_writer::serialize_map_cell_path_key_into;
    let mut out = Vec::new();
    serialize_map_cell_path_key_into(
        &Value::Empty(EmptyValueType::Int),
        "map<int, int>",
        &mut out,
    )
    .expect("map<int,int> must admit an Empty(int) cell path key");
    assert!(
        out.is_empty(),
        "the admitted sentinel must write ZERO bytes, wrote {}",
        out.len()
    );
}

/// PIN for `serialize_value_into` — the type-blind writer refuses every family.
#[test]
fn the_type_blind_writer_refuses_the_sentinel() {
    use crate::storage::sstable::writer::data_writer::serialize_value_into;
    for tag in every_tag() {
        let mut out = Vec::new();
        let err = serialize_value_into(&Value::Empty(tag), &mut out)
            .expect_err("the type-blind writer must refuse the sentinel");
        assert!(
            err.to_string().contains("#3805"),
            "the refusal must name #3805: {err}"
        );
        assert!(out.is_empty(), "a refused write must append nothing");
    }
}

/// PIN for `TypeSerializer::serialize_value` (direct) and
/// `serialize_typed_value` (via a `list<int>` element — the recursion point for
/// collection elements, tuple fields and UDT fields).
#[test]
fn the_type_aware_writer_refuses_the_sentinel() {
    let serializer = crate::storage::serialization::types::TypeSerializer::new();
    for tag in every_tag() {
        let err = serializer
            .serialize_value(&Value::Empty(tag), tag.cql_name())
            .expect_err("the general cell-value API must refuse the sentinel");
        assert!(
            err.to_string().contains("#3805"),
            "the refusal must name #3805: {err}"
        );
    }
    let nested = serializer
        .serialize_value(
            &Value::List(vec![Value::Empty(EmptyValueType::Int)]),
            "list<int>",
        )
        .expect_err("serialize_typed_value must refuse a nested sentinel");
    assert!(
        nested.to_string().contains("#3805"),
        "the nested refusal must name #3805: {nested}"
    );
}

/// PIN for `parser::types::serialize_cql_value` (direct) and
/// `serialize_value_without_type_prefix` (via a list element).
#[test]
fn the_tagged_format_refuses_the_sentinel() {
    for tag in every_tag() {
        let err = crate::parser::types::serialize_cql_value(&Value::Empty(tag))
            .expect_err("the legacy tagged format must refuse the sentinel");
        assert!(
            err.to_string().contains("#4072"),
            "the tagged refusal must name #4072: {err}"
        );
    }
    let nested = crate::parser::types::serialize_cql_value(&Value::List(vec![Value::Empty(
        EmptyValueType::Int,
    )]))
    .expect_err("a nested sentinel must be refused by the untagged element serializer");
    assert!(
        nested.to_string().contains("#4072"),
        "the nested tagged refusal must name #4072: {nested}"
    );
}

/// PIN for `bti::encoder`'s `encode_value_to_buffer_with_depth`, reached through
/// the public `encode_value`.
#[test]
fn the_bti_key_encoder_refuses_the_sentinel() {
    let mut encoder = crate::storage::sstable::bti::encoder::ByteComparableEncoder::new();
    for tag in every_tag() {
        let err = encoder
            .encode_value(&Value::Empty(tag))
            .expect_err("the BTI byte-comparable key encoder must refuse the sentinel");
        assert!(
            err.to_string().contains("#3805"),
            "the BTI refusal must name #3805: {err}"
        );
    }
}

// ───────────────────────────────────────────────────────────────────────────
// (5) SPOT PINS for `RefusesNoSentinelArm` rows whose refusal is a catch-all
// ───────────────────────────────────────────────────────────────────────────
//
// These are NOT required by the disposition rules (a no-arm row's claim is the
// structural one), but a catch-all that stops erroring is worth catching, and
// these three are the crate's partition-key/clustering byte producers.

#[test]
fn the_partition_key_codec_refuses_the_sentinel() {
    for tag in every_tag() {
        assert!(
            crate::storage::partition_key_codec::encode_single_component_key_typed(
                &Value::Empty(tag),
                tag.cql_name(),
            )
            .is_err(),
            "a partition key component must not accept the sentinel ({})",
            tag.cql_name()
        );
    }
}

#[test]
fn the_clustering_serializer_refuses_the_sentinel() {
    use crate::storage::sstable::writer::data_writer::serialize_value_for_clustering;
    use crate::types::ComparatorType;
    for tag in every_tag() {
        assert!(
            serialize_value_for_clustering(&Value::Empty(tag), &ComparatorType::Int).is_err(),
            "a clustering component must not accept the sentinel ({})",
            tag.cql_name()
        );
    }
}

#[test]
fn a_regular_cell_value_refuses_the_sentinel_through_the_writer() {
    use crate::storage::sstable::writer::data_writer::write_cell_value_into;
    for tag in every_tag() {
        let mut buf = Vec::new();
        assert!(
            write_cell_value_into(&mut buf, "c", &Value::Empty(tag)).is_err(),
            "a regular cell value must not accept the sentinel ({})",
            tag.cql_name()
        );
    }
}

// ───────────────────────────────────────────────────────────────────────────
// (6) THE SCANNER ITSELF — self-tested, not trusted
// ───────────────────────────────────────────────────────────────────────────

/// The structural check in (3) is a `contains("Value::Empty")` over a function
/// BODY, which would be a text-grep false-positive machine if the scanner read
/// prose. It does not: comments, string literals (raw and byte forms) and char
/// literals are blanked first, so only PATTERN ARMS and CONSTRUCTIONS count —
/// and a `'{'` char literal or a `"}"` in a diagnostic cannot unbalance the
/// brace matching that delimits one body from the next.
///
/// Both directions are asserted: prose must NOT count (else every refusing
/// function with a doc mention would be mislabelled) and a real arm MUST count
/// (else the whole census greens vacuously).
#[test]
fn the_scanner_reads_pattern_arms_and_not_prose() {
    let text = r##"
/// A doc comment naming Value::Empty, plus a }} unbalanced-looking brace.
fn prose_only(value: &Value) -> Result<Vec<u8>> {
    // Value::Empty in a line comment
    /* Value::Empty in a block comment { with a brace */
    let msg = "Value::Empty(int) in a string with a } brace";
    let raw = r#"Value::Empty in a raw string"#;
    let open_brace = '{';
    let quote = '"';
    let _ = (msg, raw, open_brace, quote);
    Ok(Vec::new())
}

fn real_arm(value: &Value, out: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Empty(_) => Ok(()),
        _ => {
            out.clear();
            Ok(())
        }
    }
}

fn not_a_serializer(value: &Value) -> bool {
    matches!(value, Value::Empty(_))
}
"##;
    let found = serializers_in(text);
    let names: Vec<&str> = found.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        vec!["prose_only", "real_arm"],
        "the scanner must find exactly the two BYTE-PRODUCING functions — \
         `not_a_serializer` returns `bool` and is not one"
    );

    let prose_body = &found[0].1;
    assert!(
        !prose_body.contains("Value::Empty"),
        "a Value::Empty mentioned only in comments, strings or raw strings must NOT count as a \
         pattern arm; body was: {prose_body}"
    );
    assert!(
        prose_body.contains("Ok(Vec::new())"),
        "brace matching must reach the end of the body despite a '{{' char literal and a '}}' \
         inside a string; body was: {prose_body}"
    );

    let arm_body = &found[1].1;
    assert!(
        arm_body.contains("Value::Empty"),
        "a REAL `Value::Empty` pattern arm must count, or the census greens vacuously; body \
         was: {arm_body}"
    );
}
