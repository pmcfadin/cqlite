//! Issue #4501: `dump_position_convention` (tests/support/partition_start.rs) must hold
//! ONE convention per golden and reject everything else.

#[path = "support/partition_start.rs"]
mod partition_start;

use partition_start::{dump_position_convention, header_len_nb, DumpPositionConvention};

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
