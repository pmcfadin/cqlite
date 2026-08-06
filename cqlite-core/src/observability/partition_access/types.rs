//! Value types for the bounded partition access-distribution probe (issue #2827):
//! the closed bucket/size-source label sets, the per-access byte weight, and the
//! summary of one closed measurement window.
//!
//! Split out of `mod.rs` to keep each file inside the campsite-rule source target
//! (#1116): this file owns the vocabulary the probe EMITS, while `mod.rs` owns the
//! recorder that produces it and `table.rs` owns the counting structure.

/// The six repeat-access buckets, verbatim as specified. A closed set — the whole
/// cardinality budget of the emitted series.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RepeatBucket {
    /// Accessed exactly once in the window.
    One,
    /// Accessed exactly twice.
    Two,
    /// Accessed 3 or 4 times.
    ThreeToFour,
    /// Accessed 5–8 times.
    FiveToEight,
    /// Accessed 9–16 times.
    NineToSixteen,
    /// Accessed 17 or more times.
    SeventeenPlus,
}

impl RepeatBucket {
    /// Every bucket, in ascending order.
    pub const ALL: [RepeatBucket; 6] = [
        RepeatBucket::One,
        RepeatBucket::Two,
        RepeatBucket::ThreeToFour,
        RepeatBucket::FiveToEight,
        RepeatBucket::NineToSixteen,
        RepeatBucket::SeventeenPlus,
    ];

    /// The bounded attribute value for `cqlite.read.repeat_bucket`.
    pub fn label(self) -> &'static str {
        match self {
            RepeatBucket::One => "1",
            RepeatBucket::Two => "2",
            RepeatBucket::ThreeToFour => "3-4",
            RepeatBucket::FiveToEight => "5-8",
            RepeatBucket::NineToSixteen => "9-16",
            RepeatBucket::SeventeenPlus => "17+",
        }
    }

    /// Classify a repeat count. `0` is not a valid input (an entry exists only
    /// because it was accessed at least once) and is classified as [`Self::One`].
    pub fn from_count(count: u32) -> Self {
        match count {
            0 | 1 => RepeatBucket::One,
            2 => RepeatBucket::Two,
            3..=4 => RepeatBucket::ThreeToFour,
            5..=8 => RepeatBucket::FiveToEight,
            9..=16 => RepeatBucket::NineToSixteen,
            _ => RepeatBucket::SeventeenPlus,
        }
    }

    pub(super) fn index(self) -> usize {
        match self {
            RepeatBucket::One => 0,
            RepeatBucket::Two => 1,
            RepeatBucket::ThreeToFour => 2,
            RepeatBucket::FiveToEight => 3,
            RepeatBucket::NineToSixteen => 4,
            RepeatBucket::SeventeenPlus => 5,
        }
    }
}

/// Provenance of an access's on-disk byte weight — the closed
/// `cqlite.read.size_source` value set of THREE values.
///
/// A reader must always be able to tell a MEASURED weight from a weight the index
/// handed over, and both from a genuinely unknown one — so the provenance is a
/// distinct label rather than being folded into `index`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeSource {
    /// Every SSTable resolved for the access reported a size directly in its index
    /// metadata (`PartitionLoc.data_size`). No Cassandra 5.0 index format records
    /// one, so this is unreachable for Cassandra-written SSTables today; it is kept
    /// because a producer that genuinely knows a size must not be forced to report
    /// a measured one.
    Index,
    /// The weight was MEASURED as the partition's successor gap —
    /// `[data_offset, successor_offset)`, bounding to the authoritative uncompressed
    /// data-section length for the last partition. Authoritative index-layout
    /// metadata, the same bound the single-partition seek uses to size its decode
    /// window; never an estimate.
    SuccessorGap,
    /// At least one resolved SSTable yielded no authoritative extent at all. The
    /// access is still counted; it contributes ZERO bytes.
    Unavailable,
}

impl SizeSource {
    /// Every value, in order.
    pub const ALL: [SizeSource; 3] = [
        SizeSource::Index,
        SizeSource::SuccessorGap,
        SizeSource::Unavailable,
    ];

    /// The bounded attribute value for `cqlite.read.size_source`.
    pub fn label(self) -> &'static str {
        match self {
            SizeSource::Index => "index",
            SizeSource::SuccessorGap => "successor_gap",
            SizeSource::Unavailable => "unavailable",
        }
    }
}

/// The on-disk byte weight of one logical partition access.
///
/// **`Unavailable` fails closed and is never filled in.** A resolution that yields
/// no size — BTI trie resolution records only an offset
/// (`PartitionLoc::offset_only`, `data_size = 0`), and see the reachability note
/// below — has no authoritative weight. Such an access is still COUNTED
/// as a partition access — dropping it would make the histogram itself wrong — but
/// it contributes ZERO bytes and is reported under
/// `distinct_partitions{cqlite.read.size_source="unavailable"}`, so an incomplete
/// byte total always has a visible `unavailable` series beside it and the decision
/// procedure can refuse the window. A size is never estimated, interpolated from a
/// successor offset, or defaulted to a nominal value (no-heuristics, #28).
///
/// # Reachability of [`AccessWeight::Index`] today — an OPEN finding on #2827
///
/// The approved design assumed the BIG `Index.db` supplies a per-partition size.
/// It does not: a Cassandra 5.0 BIG index entry is
/// `[key][data_offset vint][promoted_index_len vint][promoted_index]`
/// (`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`), with no
/// size field, which is why the reader's own seek path bounds a partition with the
/// SUCCESSOR offset instead. So `PartitionLoc.data_size` is `0` for BIG as well as
/// BTI, and in practice every access resolves to [`AccessWeight::Unavailable`]:
/// the histogram is fully live, the byte weighting is not, and the decision
/// procedure consequently refuses every window on its unpriceable-fraction
/// condition. The successor gap the read path already computes is the available
/// authoritative extent; wiring it was explicitly deferred by the approved design
/// and is not taken unilaterally here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessWeight {
    /// On-disk bytes taken directly from index-recorded sizes, summed across the
    /// SSTables resolved for this one logical access.
    Index(u64),
    /// On-disk bytes MEASURED as the successor gap, summed across the SSTables
    /// resolved for this one logical access.
    SuccessorGap(u64),
    /// No authoritative extent was available for at least one resolved SSTable.
    Unavailable,
}

impl AccessWeight {
    pub(super) fn bytes(self) -> Option<u64> {
        match self {
            AccessWeight::Index(b) | AccessWeight::SuccessorGap(b) => Some(b),
            AccessWeight::Unavailable => None,
        }
    }

    pub(super) fn source(self) -> SizeSource {
        match self {
            AccessWeight::Index(_) => SizeSource::Index,
            AccessWeight::SuccessorGap(_) => SizeSource::SuccessorGap,
            AccessWeight::Unavailable => SizeSource::Unavailable,
        }
    }
}

/// Accumulates the byte weight of ONE logical partition access across the SSTables
/// that access resolved.
///
/// Fails closed in both directions: an accumulator that saw no authoritative size
/// at all finishes as [`AccessWeight::Unavailable`] (an access with nothing to
/// price is not an access priced at zero), and a single unsized SSTable poisons the
/// whole access.
#[derive(Clone, Copy, Debug, Default)]
pub struct AccessWeightBuilder {
    bytes: u64,
    sized: u32,
    measured: bool,
    unavailable: bool,
}

impl AccessWeightBuilder {
    /// A fresh accumulator for one logical access.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that one resolved SSTable reported `data_size` on-disk bytes for this
    /// partition. A `data_size` of `0` is NOT a size — it is how BTI resolution
    /// records "the trie knows the offset and nothing else" — so it is folded in as
    /// [`Self::note_unsized`].
    pub fn note_sized(&mut self, data_size: u32) {
        if data_size == 0 {
            self.note_unsized();
            return;
        }
        self.bytes = self.bytes.saturating_add(u64::from(data_size));
        self.sized = self.sized.saturating_add(1);
    }

    /// Record that one resolved SSTable's extent was MEASURED as its successor gap
    /// (`[data_offset, successor_offset)`, or the data-section-length bound for the
    /// last partition). A zero-length gap is not an extent and folds to
    /// [`Self::note_unsized`].
    pub fn note_measured(&mut self, gap_bytes: u64) {
        if gap_bytes == 0 {
            self.note_unsized();
            return;
        }
        self.bytes = self.bytes.saturating_add(gap_bytes);
        self.sized = self.sized.saturating_add(1);
        self.measured = true;
    }

    /// Record that one resolved SSTable yielded no authoritative extent.
    pub fn note_unsized(&mut self) {
        self.unavailable = true;
    }

    /// Finish the accumulation.
    ///
    /// Provenance is the WEAKEST of the contributions: a total that mixes an
    /// index-recorded size with a measured gap is reported as measured, because the
    /// total is only as well-founded as its weakest component. Any unpriceable
    /// contribution — or no contribution at all — makes the whole access
    /// unavailable.
    pub fn finish(self) -> AccessWeight {
        if self.unavailable || self.sized == 0 {
            AccessWeight::Unavailable
        } else if self.measured {
            AccessWeight::SuccessorGap(self.bytes)
        } else {
            AccessWeight::Index(self.bytes)
        }
    }
}

/// Per-bucket totals for one closed window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BucketStats {
    /// Distinct partitions in this bucket whose bytes came from an index-recorded
    /// size.
    pub distinct_index: u64,
    /// Distinct partitions in this bucket whose bytes were MEASURED as the
    /// successor gap.
    pub distinct_successor_gap: u64,
    /// Distinct partitions in this bucket whose bytes could not be priced at all.
    pub distinct_unavailable: u64,
    /// Sum of the repeat counts of every partition in this bucket.
    pub accesses: u64,
    /// Sum of DISTINCT-partition on-disk bytes in this bucket (unavailable
    /// partitions contribute zero, by construction).
    pub bytes: u64,
}

impl BucketStats {
    /// Distinct partitions in this bucket, priced or not.
    pub fn distinct(&self) -> u64 {
        self.distinct_index + self.distinct_successor_gap + self.distinct_unavailable
    }

    /// Distinct partitions in this bucket that carry an authoritative extent.
    pub fn distinct_priced(&self) -> u64 {
        self.distinct_index + self.distinct_successor_gap
    }
}

/// The summary of one CLOSED measurement window — the complete input to the
/// decision procedure.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WindowSummary {
    pub(super) buckets: [BucketStats; 6],
    /// `2^k` for the sampling prefix width `k` in force at close. `1` = census.
    pub sample_denominator: u64,
    /// The recorder hit its sampling floor: the surviving sample is too small to
    /// be worth anything and the decision procedure refuses the window.
    pub at_sampling_floor: bool,
    /// Every access the recorder was asked to record, including accesses to keys
    /// the sampling predicate did not admit. Always `>= total_accesses()`.
    pub recorded_accesses: u64,
}

impl WindowSummary {
    /// Stats for one bucket.
    pub fn bucket(&self, b: RepeatBucket) -> BucketStats {
        self.buckets[b.index()]
    }

    /// `A = Σ a_b` — accesses attributable to the admitted sample.
    pub fn total_accesses(&self) -> u64 {
        self.buckets.iter().map(|b| b.accesses).sum()
    }

    /// Distinct partitions in the admitted sample.
    pub fn distinct_partitions(&self) -> u64 {
        self.buckets.iter().map(|b| b.distinct()).sum()
    }

    /// Distinct partitions whose on-disk bytes could not be priced.
    pub fn unavailable_partitions(&self) -> u64 {
        self.buckets.iter().map(|b| b.distinct_unavailable).sum()
    }

    /// Total distinct-partition on-disk bytes across every bucket.
    pub fn total_bytes(&self) -> u64 {
        self.buckets.iter().map(|b| b.bytes).sum()
    }

    /// A census window counted every distinct partition it saw.
    pub fn is_census(&self) -> bool {
        self.sample_denominator == 1
    }

    /// Fraction of distinct partitions whose bytes could not be priced. `0.0` for
    /// an empty window.
    pub fn unavailable_fraction(&self) -> f64 {
        let total = self.distinct_partitions();
        if total == 0 {
            return 0.0;
        }
        self.unavailable_partitions() as f64 / total as f64
    }
}
