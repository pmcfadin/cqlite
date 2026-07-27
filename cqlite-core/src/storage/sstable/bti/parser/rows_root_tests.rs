//! Unit tests for the `Rows.db` row-index root validation (issue #3002).
//!
//! Split out of `rows_root.rs` per the campsite rule (`#[path]`-included).

use super::super::node_decode::parse_bti_node;
use super::super::partitions::payload_start_in_node;
use super::*;

/// `[header][SizedInts block offset]` — a `PayloadOnly` (ordinal 0) row-index leaf
/// with a 1-byte block offset (`payloadBits = 1`).
fn payload_only_leaf(block_offset: u8) -> Vec<u8> {
    vec![0x01, block_offset]
}

/// `Single8` (ordinal 2) with `payloadBits` and a 1-byte backward delta:
/// `[0x20|pf][transition][delta]` (+ payload bytes appended by the caller).
fn single8(payload_flags: u8, transition: u8, delta: u8) -> Vec<u8> {
    vec![0x20 | (payload_flags & 0x0F), transition, delta]
}

/// `SingleNoPayload4` (ordinal 1): `[0x10|delta4][transition]`.
fn single_nopayload4(delta4: u8, transition: u8) -> Vec<u8> {
    vec![0x10 | (delta4 & 0x0F), transition]
}

/// `Sparse8` (ordinal 5): `[0x50|pf][count][transitions][1-byte deltas]`.
fn sparse8(payload_flags: u8, pairs: &[(u8, u8)]) -> Vec<u8> {
    let mut v = vec![0x50 | (payload_flags & 0x0F), pairs.len() as u8];
    v.extend(pairs.iter().map(|&(t, _)| t));
    v.extend(pairs.iter().map(|&(_, d)| d));
    v
}

/// The real-fixture shape in miniature: a payload-less `SingleNoPayload4` CHILD
/// immediately followed by the `Single8` ROOT that carries block 0's payload, then
/// the `TrieIndexEntry` at `rows_offset`.
///
/// This is exactly `test_da/wide_table` (`12 80 | 21 40 02 07 | <entry>`): the root
/// node's serialized bytes end at the entry. A `Rows.db` WRITTEN against the
/// pre-#3002 2-low base makes a correct reader resolve `root + 2` (two bytes INTO
/// the root's own body); the pre-#3002 READER resolved `root - 2` (the child).
fn fixture_shaped_rows_db() -> (Vec<u8>, usize, usize) {
    let mut db = Vec::new();
    db.extend(single_nopayload4(2, 0x80)); // child at 0..2
    let root = db.len();
    db.extend(single8(1, 0x40, 2)); // root at 2..5 (child = root - 2 = 0)
    db.push(0x07); // root payload: 1-byte SizedInts block offset 7
    let rows_offset = db.len(); // 6
    db.extend_from_slice(&[0x00, 0x00]); // a (stub) entry begins here
    (db, root, rows_offset)
}

/// The production raw-byte structure length must agree with the parsed-node layout
/// (`payload_start_in_node`) for EVERY payload-capable node family, so the two can
/// never drift. Ordinals 1/3 (`SingleNoPayload`) are excluded deliberately:
/// `payload_start_in_node`'s `Single` arm is documented as unreachable for them (it
/// would report `node_offset + 2` for the 3-byte ordinal 3), and
/// `validate_rows_trie_root` rejects those ordinals as roots outright.
#[test]
fn structure_len_agrees_with_parsed_payload_start_for_every_family() {
    // (label, node bytes) — one representative per pointer-width encoding.
    let mut cases: Vec<(&str, Vec<u8>)> = vec![
        ("PayloadOnly", vec![0x01]),
        ("Single8", single8(1, b'a', 3)),
        ("Single16", vec![0x41, b'a', 0x01, 0x00]),
        ("Sparse8", sparse8(1, &[(b'a', 2), (b'b', 4)])),
        // Sparse12 (ordinal 6): 2 + count + ceil(count*3/2)
        ("Sparse12", vec![0x61, 0x02, b'a', b'b', 0x00, 0x10, 0x02]),
        // Sparse16 (ordinal 7)
        ("Sparse16", vec![0x71, 0x01, b'a', 0x00, 0x02]),
        // Sparse24 (ordinal 8)
        ("Sparse24", vec![0x81, 0x01, b'a', 0x00, 0x00, 0x02]),
        // Sparse40 (ordinal 9)
        ("Sparse40", vec![0x91, 0x01, b'a', 0, 0, 0, 0, 2]),
        // Dense12 (ordinal 10): 3 + ceil(range*3/2), range = byte2 + 1
        ("Dense12", vec![0xA1, b'a', 0x01, 0x00, 0x10, 0x02]),
        // Dense16 (ordinal 11)
        ("Dense16", vec![0xB1, b'a', 0x00, 0x00, 0x02]),
        // Dense24 (ordinal 12)
        ("Dense24", vec![0xC1, b'a', 0x00, 0x00, 0x00, 0x02]),
        // Dense32 (ordinal 13)
        ("Dense32", vec![0xD1, b'a', 0x00, 0, 0, 0, 2]),
        // Dense40 (ordinal 14)
        ("Dense40", vec![0xE1, b'a', 0x00, 0, 0, 0, 0, 2]),
        // LongDense (ordinal 15)
        ("LongDense", vec![0xF1, b'a', 0x00, 0, 0, 0, 0, 0, 0, 0, 2]),
    ];
    // Also cover the payload-less internal shape CQLite's own writer emits.
    cases.push(("Sparse8 payloadBits=0", sparse8(0, &[(0x40, 2)])));

    for (label, node) in cases {
        // Place the node at a non-zero offset so offsets are exercised, and append a
        // 1-byte SizedInts payload (consumed only when payloadBits != 0).
        let node_offset = 4usize;
        let mut db = vec![0x00; node_offset];
        db.extend_from_slice(&node);
        db.push(0x07); // the payload byte (or trailing filler for payloadBits=0)

        let structure_len =
            node_structure_len(&db, node_offset).unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(
            structure_len,
            node.len(),
            "{label}: raw-byte structure length must equal the crafted node's own length"
        );

        // `PayloadOnly` (ordinal 0) is excluded from the parsed-node cross-check:
        // `parse_bti_node` decodes its payload as the LEGACY 12-byte `PayloadRef`
        // (the partition-index form), which a 2-byte `Rows.db` `IndexInfo` leaf is
        // not — the Rows.db payload reader handles ordinal 0 without parsing the
        // node at all (`read_row_node_payload`). Its structure length is the bare
        // header byte, asserted directly.
        if db[node_offset] >> 4 == 0 {
            assert_eq!(
                structure_len, 1,
                "{label}: PayloadOnly is a lone header byte"
            );
        } else {
            let parsed = parse_bti_node(&db[node_offset..], node_offset as u64)
                .unwrap_or_else(|e| panic!("{label} must parse: {e}"));
            let payload_start = payload_start_in_node(&parsed, &db, node_offset)
                .unwrap_or_else(|e| panic!("{label}: payload_start_in_node: {e}"));
            assert_eq!(
                payload_start,
                node_offset + structure_len,
                "{label}: the raw-byte structure length must agree with the parsed-node \
                 layout (payload_start_in_node) — a drift means one was updated alone"
            );
        }

        // The extent adds the 1-byte payload exactly when payloadBits != 0, and is
        // UNAMBIGUOUS: none of these payloads carries an open-marker DeletionTime.
        let payload_bits = node[0] & 0x0F;
        let expected_end = node_offset + structure_len + if payload_bits == 0 { 0 } else { 1 };
        let extent = rows_node_serialized_extent(&db, node_offset)
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(
            (extent.shortest_end(), extent.is_ambiguous()),
            (expected_end, false),
            "{label}: extent = structure + IndexInfo payload, with a single legal end"
        );
    }
}

/// An open range-tombstone marker (`FLAG_OPEN_MARKER`) extends the payload by the
/// modern DA `DeletionTime`: 1 byte for the LIVE sentinel, 12 for a real value.
#[test]
fn extent_includes_the_open_marker_deletion_time() {
    // payloadBits = 1 | FLAG_OPEN_MARKER = 0x9 → 1 SizedInts byte + DeletionTime.
    let live = {
        let mut db = single8(0x9, b'a', 1);
        db.push(0x07); // block offset
        db.push(0x80); // LIVE sentinel (1 byte)
        db
    };
    let extent = rows_node_serialized_extent(&live, 0).expect("live open marker");
    // The 12-byte alternative does not fit in this 5-byte buffer, so there is no
    // ambiguity to report here.
    assert_eq!((extent.shortest_end(), extent.is_ambiguous()), (5, false));

    let non_live = {
        let mut db = single8(0x9, b'a', 1);
        db.push(0x07);
        db.extend_from_slice(&17i64.to_be_bytes()); // markedForDeleteAt
        db.extend_from_slice(&9u32.to_be_bytes()); // localDeletionTime
        db
    };
    let extent = rows_node_serialized_extent(&non_live, 0).expect("non-live open marker");
    assert_eq!(
        (extent.shortest_end(), extent.is_ambiguous()),
        (3 + 1 + 12, false),
        "a body whose leading byte is not 0x80 has exactly one legal length"
    );
}

/// FALSE-REJECTION GUARD (roborev #3002): an open-marker `DeletionTime` whose
/// `markedForDeleteAt` MSB is `0x80` is prefix-indistinguishable from the 1-byte LIVE
/// sentinel, so measuring the payload by the sentinel-first decode alone would come
/// up 11 bytes SHORT and reject a VALID file (degrading every clustering slice on it
/// to a full-partition scan). Both structurally possible ends are reported, and the
/// root still validates.
#[test]
fn open_marker_deletion_time_starting_with_0x80_still_validates() {
    // `markedForDeleteAt` in the Long.MIN_VALUE octant: BE bytes start with 0x80.
    let marked_for_delete_at = i64::MIN + 7;
    assert_eq!(marked_for_delete_at.to_be_bytes()[0], 0x80);

    let mut db = single8(0x9, b'a', 1); // ordinal 2, payloadBits = 1 | FLAG_OPEN_MARKER
    db.push(0x07); // 1-byte SizedInts block offset
    db.extend_from_slice(&marked_for_delete_at.to_be_bytes());
    db.extend_from_slice(&9u32.to_be_bytes());
    let rows_offset = db.len(); // the entry starts right after the 12-byte body
    db.extend_from_slice(&[0x00, 0x00]); // a (stub) entry

    let extent = rows_node_serialized_extent(&db, 0).expect("the node measures");
    assert_eq!(
        (extent.shortest_end(), extent.is_ambiguous()),
        (5, true),
        "0x80 is both the LIVE sentinel and the MSB of this body, so BOTH ends are legal"
    );
    assert!(
        extent.ends_at(rows_offset),
        "the 12-byte reading ends at the entry"
    );

    assert_eq!(
        validate_rows_trie_root(&db, 0, rows_offset).map(|r| r.offset()),
        Ok(0),
        "a VALID open-marker root must not be rejected because its DeletionTime body \
         happens to begin with the LIVE-sentinel byte"
    );
}

/// A root whose serialized extent ends exactly at the entry validates, and the
/// capability type carries that offset.
#[test]
fn validate_accepts_the_last_written_node_as_root() {
    let (db, root, rows_offset) = fixture_shaped_rows_db();
    let validated = validate_rows_trie_root(&db, root as i64, rows_offset)
        .expect("the last-written node before the entry IS the root");
    assert_eq!(validated.offset(), root);
}

/// HEADLINE (issue #3002), BOTH mis-basing directions, neither of which the old
/// `root < rows_db.len()` bounds check caught:
///
/// - a `Rows.db` WRITTEN against the 2-low base (pre-fix CQLite) makes a correct
///   reader resolve `root + 2` — two bytes INTO the root node's own body, where the
///   delta byte `0x02` reads as a plausible `PayloadOnly` node whose extent
///   overshoots the entry;
/// - the pre-fix READER's own base resolved `root - 2`, landing on the root's
///   payload-less `SingleNoPayload4` child (the real `test_da/wide_table` shape).
#[test]
fn validate_rejects_both_two_byte_mis_basings() {
    let (db, root, rows_offset) = fixture_shaped_rows_db();

    // Written against the 2-low base → resolved 2 bytes HIGH.
    let two_high = root as i64 + 2;
    let rejection = validate_rows_trie_root(&db, two_high, rows_offset)
        .expect_err("a root 2 bytes into the root's own body must be rejected");
    assert_eq!(rejection.resolved_offset, two_high);
    assert_eq!(rejection.rows_offset, rows_offset);
    assert_eq!(
        rejection.reason,
        RowsTrieRootRejectReason::ExtentNotAtEntry {
            extent_end: rows_offset + 1
        },
        "the node the delta byte fakes ends PAST the entry, so it is not the root"
    );
    // The old check would have passed: the offset IS inside the file.
    assert!((two_high as usize) < db.len());

    // The pre-fix reader's base → resolved 2 bytes LOW, onto the payload-less child.
    let two_low = root as i64 - 2;
    assert_eq!(
        validate_rows_trie_root(&db, two_low, rows_offset)
            .expect_err("the root's child is not a root")
            .reason,
        RowsTrieRootRejectReason::PayloadIncapableNodeType { header_byte: 0x12 }
    );
}

/// A mis-based root landing on a payload-CAPABLE node type is still rejected,
/// because its extent does not end at the entry — the writer-ordering invariant is
/// what carries the detection, not the node type alone.
#[test]
fn validate_rejects_a_payload_capable_node_whose_extent_misses_the_entry() {
    // Leaf at 0..2, root Sparse8 (payloadBits = 0) at 2..6, entry at 6.
    let mut db = payload_only_leaf(0x07);
    let root = db.len();
    db.extend(sparse8(0, &[(0x40, 2)])); // 2 + 1 + 1 = 4 bytes → ends at 6
    let rows_offset = db.len();
    db.extend_from_slice(&[0x00, 0x00]);

    assert_eq!(
        validate_rows_trie_root(&db, root as i64, rows_offset).map(|r| r.offset()),
        Ok(root),
        "the payload-LESS internal root CQLite's own writer emits must validate"
    );

    // Offset 0 is a payload-capable PayloadOnly node, but its extent ends at 2.
    let rejection = validate_rows_trie_root(&db, 0, rows_offset)
        .expect_err("a node that is not the last one written must be rejected");
    assert_eq!(
        rejection.reason,
        RowsTrieRootRejectReason::ExtentNotAtEntry { extent_end: 2 }
    );
}

/// The root must lie in the trie region strictly BELOW its entry.
#[test]
fn validate_rejects_a_root_at_or_after_the_entry() {
    let (db, _root, rows_offset) = fixture_shaped_rows_db();
    for candidate in [-1i64, rows_offset as i64, rows_offset as i64 + 1, 1 << 40] {
        assert_eq!(
            validate_rows_trie_root(&db, candidate, rows_offset)
                .expect_err("out-of-region root must be rejected")
                .reason,
            RowsTrieRootRejectReason::NotBelowEntry,
            "resolved offset {candidate} is not below the entry at {rows_offset}"
        );
    }
}

/// A node whose declared shape or payload runs past EOF is rejected as truncated,
/// never panicked on.
#[test]
fn validate_rejects_a_truncated_node() {
    // Sparse8 claiming 200 transitions with nothing following it.
    let db = vec![0x51u8, 200, 0x40];
    let rejection = validate_rows_trie_root(&db, 0, db.len())
        .expect_err("a node claiming more bytes than the file holds must be rejected");
    assert_eq!(rejection.reason, RowsTrieRootRejectReason::TruncatedNode);

    // A payload-bearing node whose SizedInts payload is cut off.
    let db = single8(1, b'a', 1); // 3 bytes, payloadBits = 1, no payload byte
    assert_eq!(
        validate_rows_trie_root(&db, 0, db.len())
            .expect_err("a cut-off payload must be rejected")
            .reason,
        RowsTrieRootRejectReason::TruncatedNode
    );
}

/// A zero-transition `Sparse` node is structurally impossible (`TrieNode.Sparse`
/// always stores >= 1 transition), and it is reported as the SHAPE violation it is —
/// not as a truncation, which would tell an operator their intact file is cut short.
#[test]
fn validate_rejects_a_zero_transition_sparse_node_as_a_shape_violation() {
    let db = vec![0x51u8, 0x00, 0x00, 0x00];
    let reason = validate_rows_trie_root(&db, 0, db.len())
        .expect_err("a zero-transition Sparse node must be rejected")
        .reason;
    assert_eq!(
        reason,
        RowsTrieRootRejectReason::SparseNodeWithoutTransitions
    );
    let message = reason.to_string();
    assert!(
        message.contains("0 transitions") && !message.contains("truncated"),
        "the message must name the violated Sparse invariant, not truncation: {message}"
    );
}

/// A `0x00` byte one byte before the entry is a `PayloadOnly` (childless) node with
/// `payloadBits == 0`: it encodes neither a transition nor an `IndexInfo`, so
/// `TrieNode.typeFor` never emits it. Its 1-byte extent would otherwise satisfy the
/// extent equality by accident and be traversed as a root (roborev #3002).
#[test]
fn validate_rejects_a_childless_payload_less_root() {
    // [PayloadOnly leaf][0x00][entry] — the 0x00 sits at `rows_offset - 1`.
    let mut db = payload_only_leaf(0x07);
    let bogus_root = db.len();
    db.push(0x00);
    let rows_offset = db.len();
    db.extend_from_slice(&[0x00, 0x00]); // a (stub) entry

    // The extent equality alone would ACCEPT it: the node "ends" exactly at the entry.
    let extent = rows_node_serialized_extent(&db, bogus_root).expect("a lone header byte");
    assert_eq!(extent.shortest_end(), rows_offset);

    let rejection = validate_rows_trie_root(&db, bogus_root as i64, rows_offset)
        .expect_err("a node that encodes nothing cannot root a row index");
    assert_eq!(
        rejection.reason,
        RowsTrieRootRejectReason::ChildlessRootWithoutPayload { header_byte: 0x00 }
    );
}

/// Every rejection reason has a DISTINCT, stable metric label (the bounded attribute
/// value set carried on `cqlite.read.bti.rows_root_rejected`).
#[test]
fn every_reject_reason_has_a_distinct_stable_label() {
    let reasons = [
        RowsTrieRootRejectReason::NotBelowEntry,
        RowsTrieRootRejectReason::PayloadIncapableNodeType { header_byte: 0x12 },
        RowsTrieRootRejectReason::ChildlessRootWithoutPayload { header_byte: 0x00 },
        RowsTrieRootRejectReason::TruncatedNode,
        RowsTrieRootRejectReason::SparseNodeWithoutTransitions,
        RowsTrieRootRejectReason::InvalidPayloadBits { payload_bits: 0x8 },
        RowsTrieRootRejectReason::ExtentNotAtEntry { extent_end: 7 },
    ];
    let mut labels = std::collections::BTreeSet::new();
    for reason in reasons {
        let label = reason.label();
        assert!(
            !label.is_empty() && label.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "{label:?} must be a bounded snake_case attribute value"
        );
        assert!(labels.insert(label), "duplicate label {label}");
    }
    assert_eq!(
        labels.len(),
        7,
        "a new reason variant must be given its own label (the metric attribute's \
         value set is closed): {labels:?}"
    );
}

/// The rejection is a real `std::error::Error`, so it composes with `?` in a function
/// returning a boxed/`anyhow` error — it sits in an `Err` position.
#[test]
fn rejection_is_a_std_error() {
    let (db, root, rows_offset) = fixture_shaped_rows_db();
    fn require_root(
        db: &[u8],
        root: i64,
        rows_offset: usize,
    ) -> std::result::Result<usize, Box<dyn std::error::Error>> {
        Ok(validate_rows_trie_root(db, root, rows_offset)?.offset())
    }
    assert_eq!(
        require_root(&db, root as i64, rows_offset).map_err(|e| e.to_string()),
        Ok(root)
    );
    let message = require_root(&db, root as i64 - 2, rows_offset)
        .expect_err("the child is not a root")
        .to_string();
    assert!(
        message.contains("is unusable") && message.contains("SingleNoPayload"),
        "the boxed error must carry the full diagnostic: {message}"
    );
}

/// `payloadBits` that cannot describe a `RowIndexReader.IndexInfo` (a 0 SizedInts
/// width once `FLAG_OPEN_MARKER` is masked off) is a structural rejection, not a
/// silently accepted extent.
#[test]
fn validate_rejects_invalid_payload_bits() {
    // payloadBits = 0x8 → FLAG_OPEN_MARKER set with a 0-byte block offset.
    let mut db = single8(0x8, b'a', 1);
    db.extend_from_slice(&[0x80, 0x00]);
    let rows_offset = db.len();
    assert_eq!(
        validate_rows_trie_root(&db, 0, rows_offset)
            .expect_err("a 0-width IndexInfo offset must be rejected")
            .reason,
        RowsTrieRootRejectReason::InvalidPayloadBits { payload_bits: 0x8 }
    );
}

/// The test-only extent hook mirrors the internal helper (it is what the
/// writer-side canonical-base test cross-checks against).
#[test]
fn extent_test_hook_matches_the_internal_helper() {
    let (db, root, rows_offset) = fixture_shaped_rows_db();
    assert_eq!(
        rows_node_serialized_extent_end_for_test(&db, root),
        Some(rows_offset)
    );
    assert_eq!(
        rows_node_serialized_extent_end_for_test(&db, db.len()),
        None
    );
}
