//! Mutation completeness semantics for bodies whose final update is only
//! structurally decoded.

use super::{decode_mutation, SchemaSet};

fn mutation_with_updates(declared_count: u8, updates: &[Vec<u8>]) -> Vec<u8> {
    let mut body = vec![declared_count];
    for update in updates {
        body.extend_from_slice(update);
    }
    body
}

fn empty_partition_update(table_id: u8) -> Vec<u8> {
    let mut update = vec![table_id; 16];
    update.extend_from_slice(&[0, 0x01]); // empty key, ITER_IS_EMPTY
    update
}

fn partially_decoded_regular_update(table_id: u8) -> Vec<u8> {
    let mut update = vec![table_id; 16];
    update.push(0); // empty partition key
    update.push(0); // no iterator flags
    update.extend_from_slice(&[0, 0, 0]); // EncodingStats
    update.push(0); // parsed empty regular column-name block; schema is absent
    update
}

#[test]
fn one_final_partial_update_is_still_represented_completely() {
    let body = mutation_with_updates(1, &[partially_decoded_regular_update(1)]);
    let mutation = decode_mutation(&body, &SchemaSet::new()).expect("decode mutation");

    assert_eq!(mutation.updates.len(), 1);
    assert!(!mutation.updates[0].rows_decoded);
    assert!(mutation.updates_complete);
}

#[test]
fn partial_update_with_a_declared_suffix_makes_the_mutation_incomplete() {
    let body = mutation_with_updates(2, &[partially_decoded_regular_update(1)]);
    let mutation = decode_mutation(&body, &SchemaSet::new()).expect("decode mutation");

    assert_eq!(mutation.updates.len(), 1);
    assert!(!mutation.updates_complete);
}

#[test]
fn final_partial_update_after_a_fully_read_prefix_is_still_complete() {
    let body = mutation_with_updates(
        2,
        &[
            empty_partition_update(1),
            partially_decoded_regular_update(2),
        ],
    );
    let mutation = decode_mutation(&body, &SchemaSet::new()).expect("decode mutation");

    assert_eq!(mutation.updates.len(), 2);
    assert!(mutation.updates[0].rows_decoded);
    assert!(!mutation.updates[1].rows_decoded);
    assert!(mutation.updates_complete);
}
