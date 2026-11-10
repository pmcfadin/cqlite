//! Issue #4501: `dump_position_convention` (tests/support/partition_start.rs) must hold
//! ONE convention per golden and reject everything else.

#[path = "support/partition_start.rs"]
mod partition_start;

use partition_start::{
    assert_partition_header_at, dump_position_convention, header_len_modern_live, header_len_nb,
    DumpPositionConvention, HeaderForm,
};

#[test]
fn classifies_a_uniform_golden_either_way() {
    let h = header_len_nb(4);
    assert_eq!(
        dump_position_convention("old", &[(0, h, 0), (149, h, 149)]),
        DumpPositionConvention::PartitionStart
    );
    assert_eq!(
        dump_position_convention("new", &[(0, h, 18), (149, h, 167)]),
        DumpPositionConvention::AfterHeader
    );
}

#[test]
#[should_panic(expected = "mixed dump-position conventions")]
fn rejects_a_mixed_convention_golden() {
    let h = header_len_nb(4);
    // first partition reports its start, second reports start + header
    dump_position_convention("mixed", &[(0, h, 0), (149, h, 167)]);
}

#[test]
#[should_panic(expected = "neither the index-derived start")]
fn rejects_a_position_that_is_neither() {
    let h = header_len_nb(4);
    dump_position_convention("bad", &[(0, h, 5)]);
}

#[test]
#[should_panic(expected = "no partitions to classify")]
fn rejects_an_empty_golden() {
    dump_position_convention("empty", &[]);
}

/// A synthetic two-partition `nb` data section: [u16 4][key][live 12 bytes][row bytes ...].
fn nb_data(keys: &[i32]) -> (Vec<u8>, Vec<u64>) {
    let (mut d, mut starts) = (Vec::new(), Vec::new());
    for k in keys {
        starts.push(d.len() as u64);
        d.extend_from_slice(&4u16.to_be_bytes());
        d.extend_from_slice(&k.to_be_bytes());
        d.extend_from_slice(&i32::MAX.to_be_bytes());
        d.extend_from_slice(&i64::MIN.to_be_bytes());
        d.extend_from_slice(&[0x24, 0x00, 0x11, 0x22, 0x33, 0x44, 0x01]); // row-ish bytes
    }
    (d, starts)
}

#[test]
fn header_check_accepts_the_true_start() {
    let (d, starts) = nb_data(&[1, 2]);
    for (k, s) in [1i32, 2].iter().zip(starts) {
        assert_partition_header_at("start", &d, s, &k.to_be_bytes(), HeaderForm::Nb);
    }
}

#[test]
#[should_panic(expected = "key length at offset")]
fn header_check_rejects_start_plus_header_for_nb() {
    let (d, starts) = nb_data(&[1, 2]);
    // what 5.0.8 sstabledump reports as the first partition's `position`
    assert_partition_header_at(
        "post-header",
        &d,
        starts[0] + header_len_nb(4),
        &1i32.to_be_bytes(),
        HeaderForm::Nb,
    );
}

#[test]
#[should_panic(expected = "key length at offset")]
fn header_check_rejects_start_plus_header_for_modern_live() {
    let mut d = Vec::new();
    d.extend_from_slice(&4u16.to_be_bytes());
    d.extend_from_slice(&7i32.to_be_bytes());
    d.push(0x80);
    d.extend_from_slice(&[0x24, 0x00, 0x11, 0x22, 0x33, 0x44, 0x01, 0x00]);
    assert_partition_header_at("start", &d, 0, &7i32.to_be_bytes(), HeaderForm::ModernLive);
    assert_partition_header_at(
        "post-header",
        &d,
        header_len_modern_live(4),
        &7i32.to_be_bytes(),
        HeaderForm::ModernLive,
    );
}
