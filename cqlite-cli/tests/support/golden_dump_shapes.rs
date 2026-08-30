//! The `sstabledump` dump SHAPES that make a golden non-comparable against a
//! reconciled result set, and the scan that finds them in a golden (issue #1491).
//!
//! Split out of the `issue_1491_json_csv_golden_parity` test target under the
//! campsite rule (that file reached 1499 lines against the ~1500 test target).
//! The seam is a SUBJECT one: this module answers "what does this golden
//! CONTAIN", entirely from the dump's own vocabulary, while the test target owns
//! which tables are compared and which are excluded for what it finds here.
//!
//! Nothing here reads CQLite's output. The vocabulary — the element `type`,
//! `deletion_info`, and the `ttl`/`expires_at`/`expired` liveness keys — is
//! `sstabledump`'s, and each shape's `minimal_golden` is transcribed from shapes
//! the committed goldens actually contain.

use serde_json::Value;
use std::collections::BTreeSet;

/// A dump shape that makes a golden non-comparable against the CLI's reconciled
/// result set — and therefore a legitimate reason to exclude a table.
///
/// Every variant is a shape `super::golden_rows` REFUSES; that correspondence is
/// itself asserted by the test target's
/// `every_declarable_shape_is_one_the_golden_reader_refuses`, so the enum cannot
/// drift into listing something the reader would happily parse.
///
/// Deliberately NOT a variant: a **cell tombstone**, in either of its two dump
/// spellings. A scalar cell tombstone reconciles to `null`, which is a property
/// this lane compares rather than excludes
/// (`test_types.nb_absent_vs_null_regular`); a multicell one (a `path` plus
/// `deletion_info`) reconciles to that element being absent from its collection,
/// which `super::golden_row` reconstructs. Neither can justify an exclusion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unsupported {
    /// `partition.deletion_info`.
    PartitionDeletion,
    /// A `range_tombstone_bound` / `range_tombstone_boundary` dump element.
    RangeTombstone,
    /// A row element carrying `deletion_info`.
    RowDeletion,
    /// A `static_block` dump element.
    StaticBlock,
    /// `ttl` / `expires_at` / `expired` on a row's liveness or on a cell.
    Ttl,
}

impl Unsupported {
    /// Every variant.
    ///
    /// A HAND-KEPT list whose INTEGRITY is checked, not a derived one: Rust
    /// cannot enumerate an enum's variants without a derive macro, so no
    /// construction here can prove this list is complete. (It previously claimed
    /// to be "exhaustive by construction", which was false.) What holds instead:
    ///
    ///   * [`Self::label`] and [`Self::minimal_golden`] are EXHAUSTIVE matches, so
    ///     a new variant cannot compile without an author editing this impl — the
    ///     list is three lines above the arm they must add;
    ///   * the test target's
    ///     `every_declarable_shape_is_one_the_golden_reader_refuses` checks the list
    ///     is sorted and duplicate-free, and requires each entry's minimal golden
    ///     to carry EXACTLY that shape, so an entry cannot be a copy of its
    ///     neighbour.
    ///
    /// The residual, stated because it is real: a variant added to the enum and
    /// NOT added here is silently unchecked by that cross-check.
    pub const ALL: &'static [Unsupported] = &[
        Unsupported::PartitionDeletion,
        Unsupported::RangeTombstone,
        Unsupported::RowDeletion,
        Unsupported::StaticBlock,
        Unsupported::Ttl,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Unsupported::PartitionDeletion => "partition deletion",
            Unsupported::RangeTombstone => "range tombstone bound/boundary",
            Unsupported::RowDeletion => "row deletion marker",
            Unsupported::StaticBlock => "static block",
            Unsupported::Ttl => "TTL",
        }
    }

    /// A minimal `sstabledump` JSONL line carrying EXACTLY this shape, used to
    /// assert that the golden reader refuses it. Transcribed from the shapes the
    /// committed goldens actually contain, not from CQLite's behaviour.
    pub fn minimal_golden(self) -> &'static str {
        match self {
            Unsupported::PartitionDeletion => {
                r#"{"partition":{"key":["1"],"position":0,"deletion_info":{"marked_deleted":"1970-01-01T00:00:00.001Z","local_delete_time":"1970-01-01T00:00:00Z"}},"rows":[]}"#
            }
            Unsupported::RangeTombstone => {
                r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"range_tombstone_bound","start":{"type":"inclusive","clustering":["1"]}}]}"#
            }
            Unsupported::RowDeletion => {
                r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"row","position":1,"deletion_info":{"marked_deleted":"1970-01-01T00:00:00.001Z","local_delete_time":"1970-01-01T00:00:00Z"},"cells":[]}]}"#
            }
            Unsupported::StaticBlock => {
                r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"static_block","position":1,"cells":[{"name":"s","value":"x"}]}]}"#
            }
            Unsupported::Ttl => {
                r#"{"partition":{"key":["1"],"position":0},"rows":[{"type":"row","position":1,"liveness_info":{"tstamp":"1970-01-01T00:00:00.001Z","ttl":60,"expires_at":"1970-01-01T00:01:00Z","expired":true},"cells":[]}]}"#
            }
        }
    }
}

/// The [`Unsupported`] shapes a golden JSONL actually contains.
///
/// Read from the dump's own vocabulary — the element `type`, `deletion_info`, and
/// the `ttl`/`expires_at`/`expired` liveness keys — so the answer comes from the
/// oracle rather than from anything CQLite does with the file. An unparseable line
/// is an error, never an empty answer: "I could not tell" must not read as "no
/// unsupported shape here", which is the permissive-default shape CLAUDE.md warns
/// about.
pub fn unsupported_shapes(jsonl: &str) -> Result<BTreeSet<Unsupported>, String> {
    let mut found = BTreeSet::new();
    let ttl_keys = ["ttl", "expires_at", "expired"];
    for (lineno, line) in jsonl.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // The same strict parse `golden_rows` uses, so "this golden carries shape
        // X" and "this golden is comparable" are decided from one reading of the
        // bytes (finding K2).
        let at = || format!("golden line {}", lineno + 1);
        let doc: Value = super::strict_json::parse(line, &at())?;
        let partition = doc
            .get("partition")
            .ok_or_else(|| format!("{}: no `partition`", at()))?;
        if partition.get("deletion_info").is_some() {
            found.insert(Unsupported::PartitionDeletion);
        }
        // The same strict array read `golden_rows` uses: a `rows`/`cells` field of
        // any other JSON shape is an error, never silently zero elements — "I
        // could not tell" must not read as "no unsupported shape here".
        for row in super::array_field(&doc, "rows", &at)? {
            match row.get("type").and_then(Value::as_str) {
                Some("range_tombstone_bound") | Some("range_tombstone_boundary") => {
                    found.insert(Unsupported::RangeTombstone);
                }
                Some("static_block") => {
                    found.insert(Unsupported::StaticBlock);
                }
                Some("row") => {}
                other => {
                    return Err(format!(
                        "{}: unknown dump element type {other:?} — an unrecognised shape \
                         must be classified, not ignored",
                        at()
                    ))
                }
            }
            if row.get("deletion_info").is_some() {
                found.insert(Unsupported::RowDeletion);
            }
            if let Some(liveness) = row.get("liveness_info") {
                if ttl_keys.iter().any(|k| liveness.get(k).is_some()) {
                    found.insert(Unsupported::Ttl);
                }
            }
            for cell in super::array_field(row, "cells", &at)? {
                if ttl_keys.iter().any(|k| cell.get(k).is_some()) {
                    found.insert(Unsupported::Ttl);
                }
            }
        }
    }
    Ok(found)
}
