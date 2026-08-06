//! The fixed-footprint counting structure behind the partition access-distribution
//! probe (issue #2827, design D4).
//!
//! One lazily-allocated, open-addressed (linear-probing) table:
//!
//! - `SLOTS = 1 << 17` = 131,072
//! - entry = `key_hash: AtomicU64` (8 B) + `bytes: AtomicU64` (8 B) +
//!   `count: AtomicU32` (4 B) + `flags: AtomicU8` (1 B) + 3 B padding = **24 B**
//! - **total = 131,072 × 24 B = 3,145,728 B = exactly 3 MiB, fixed** — independent
//!   of the partition count, of qps, and of window length.
//!
//! # Why overflow is handled by SAMPLING, not eviction
//!
//! When occupancy reaches a 0.75 load factor the recorder widens a hash-prefix
//! admission predicate by one bit and drops every entry that no longer satisfies
//! it. The predicate is a function of the key hash ALONE, so it is statistically
//! independent of a key's access frequency: the survivors are a uniform random
//! sample of the DISTINCT partitions touched in the window, and each survivor's
//! count stays EXACT (the predicate is monotone — a key admitted at prefix width
//! `k` is admitted at every `k' < k` — so a downsample only ever removes keys, it
//! never invalidates a survivor's already-accumulated count).
//!
//! Recency- (LRU) or arrival-ordered eviction would do the opposite: hot keys are
//! accessed often, so they survive preferentially, the singleton bucket is
//! under-counted and the histogram OVERSTATES concentration — the direction that
//! makes a decoded-partition cache look better than it is. A go/no-go instrument
//! must not be biased toward "go", so eviction is disqualified on correctness
//! grounds, not on cost.
//!
//! # Deletion and probe chains
//!
//! Linear probing cannot simply blank a slot: later members of the same probe
//! chain would become unreachable and their counts would silently split across a
//! second entry. Downsampling therefore uses **backward-shift deletion**
//! ([`Table::delete_at`]), which restores the probe-chain invariant in place with
//! no auxiliary allocation — so the 3 MiB bound holds during a downsample too.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};

/// log2 of the slot count.
pub(super) const SLOT_BITS: u32 = 17;
/// Number of slots in the counting table.
pub(super) const SLOTS: usize = 1 << SLOT_BITS;
/// Slot index mask (`SLOTS` is a power of two).
const SLOT_MASK: usize = SLOTS - 1;
/// Declared per-entry footprint. Asserted against the real layout below.
pub(super) const ENTRY_BYTES: usize = 24;
/// The whole structure's fixed footprint: exactly 3 MiB.
pub(super) const TABLE_BYTES: usize = SLOTS * ENTRY_BYTES;
/// Occupancy at which the recorder widens the sampling prefix (load factor 0.75).
pub(super) const LOAD_FACTOR_LIMIT: usize = SLOTS / 4 * 3;
/// Longest linear-probe run before an insert reports the table effectively full.
///
/// At a 0.75 load factor with a well-mixed hash a run this long is
/// vanishingly rare; treating it as "full" simply triggers a downsample one
/// insert early, which is harmless (the scale is published).
const MAX_PROBES: usize = 64;

/// Sticky per-entry flag: at least one access to this partition resolved an
/// SSTable that reported no authoritative size, so the entry contributes ZERO
/// bytes and is counted as `size_source = unavailable`.
pub(super) const FLAG_SIZE_UNAVAILABLE: u8 = 0b0000_0001;

/// One counting slot. `key_hash == 0` means EMPTY; a hash that would be zero is
/// remapped to 1 by [`hash_key`], so the sentinel is unambiguous.
#[repr(C)]
pub(super) struct Slot {
    key_hash: AtomicU64,
    bytes: AtomicU64,
    count: AtomicU32,
    flags: AtomicU8,
    _pad: [u8; 3],
}

// The 3 MiB claim in the docs, the catalog and the spec is only true if the entry
// really is 24 bytes. Assert the layout at compile time rather than trusting the
// comment.
const _: () = assert!(std::mem::size_of::<Slot>() == ENTRY_BYTES);
const _: () = assert!(TABLE_BYTES == 3 * 1024 * 1024);

impl Slot {
    fn empty() -> Self {
        Self {
            key_hash: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            count: AtomicU32::new(0),
            flags: AtomicU8::new(0),
            _pad: [0; 3],
        }
    }

    fn clear(&self) {
        self.key_hash.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        self.flags.store(0, Ordering::Relaxed);
    }
}

/// A live entry read out of the table at window close.
#[derive(Clone, Copy, Debug)]
pub(super) struct Entry {
    /// Number of accesses recorded for this partition inside the window.
    pub(super) count: u32,
    /// Largest on-disk byte weight observed for it (meaningless when
    /// `size_unavailable` is set — such an entry contributes zero bytes).
    pub(super) bytes: u64,
    /// Sticky: at least one access could not be priced authoritatively.
    pub(super) size_unavailable: bool,
}

/// Outcome of an insert attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Insert {
    /// The access was recorded (either into a fresh slot or an existing one).
    Recorded,
    /// The key is not in the currently-admitted sample; nothing was recorded.
    NotAdmitted,
    /// No slot could be claimed within [`MAX_PROBES`]; the caller must downsample.
    Full,
}

/// The fixed 3 MiB counting table.
pub(super) struct Table {
    slots: Box<[Slot]>,
    occupancy: AtomicUsize,
}

/// A 64-bit hash of the RAW partition-key bytes the read path already holds.
///
/// Deliberately NOT the Murmur3 token: the BIG point path takes raw key bytes and
/// never computes a token, so hashing the token would make the probe force work
/// that path does not otherwise do. This value is used ONLY for slot addressing
/// and within-window identity — it is never emitted, logged, or attached to a
/// metric attribute (the whole point of the bucket histogram is that no per-key
/// value leaves the process).
///
/// FNV-1a over the bytes, finalised with a splitmix64 avalanche so the HIGH bits
/// (used by the sampling predicate) and the LOW bits (used for slot addressing)
/// are independently well-mixed. `0` is reserved as the empty-slot sentinel and is
/// remapped to `1`.
pub(super) fn hash_key(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // splitmix64 finaliser.
    let mut z = h.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    if z == 0 {
        1
    } else {
        z
    }
}

/// The hash-prefix admission predicate: a key is admitted at prefix width `k`
/// when its top `k` hash bits are all zero.
///
/// Monotone in `k` (admitted at `k` ⇒ admitted at every smaller `k`), which is
/// what makes a survivor's already-accumulated count remain exact across a
/// downsample. Uses the HIGH bits so admission is independent of the low bits that
/// choose the slot.
#[inline]
pub(super) fn admitted(hash: u64, prefix_bits: u32) -> bool {
    if prefix_bits == 0 {
        return true;
    }
    debug_assert!(prefix_bits < 64);
    (hash >> (64 - prefix_bits)) == 0
}

impl Table {
    /// Allocate the fixed 3 MiB table. Only ever called once the probe is enabled
    /// and an access actually arrives (lazy allocation — a disabled probe holds no
    /// memory at all).
    pub(super) fn new() -> Self {
        let mut slots = Vec::with_capacity(SLOTS);
        slots.resize_with(SLOTS, Slot::empty);
        Self {
            slots: slots.into_boxed_slice(),
            occupancy: AtomicUsize::new(0),
        }
    }

    /// The structure's footprint in bytes — a constant, by construction.
    pub(super) fn footprint_bytes(&self) -> usize {
        self.slots.len() * ENTRY_BYTES
    }

    /// Number of distinct keys currently held.
    pub(super) fn occupancy(&self) -> usize {
        self.occupancy.load(Ordering::Relaxed)
    }

    /// Record one access to `hash`, weighted by `bytes` (`None` = the access could
    /// not be priced authoritatively, which sets the sticky unavailable flag and
    /// contributes zero bytes).
    ///
    /// Takes `&self`: the caller holds only a READ lock, so concurrent recorders
    /// share the table and mutate their own slots with atomics. Slot claiming is a
    /// CAS on the `key_hash` sentinel, so two threads racing on the same empty slot
    /// cannot both claim it.
    pub(super) fn record(&self, hash: u64, prefix_bits: u32, bytes: Option<u64>) -> Insert {
        if !admitted(hash, prefix_bits) {
            return Insert::NotAdmitted;
        }
        let mut idx = (hash as usize) & SLOT_MASK;
        for _ in 0..MAX_PROBES {
            let slot = &self.slots[idx];
            let mut observed = slot.key_hash.load(Ordering::Acquire);
            if observed == 0 {
                match slot
                    .key_hash
                    .compare_exchange(0, hash, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => {
                        self.occupancy.fetch_add(1, Ordering::Relaxed);
                        observed = hash;
                    }
                    // Lost the race: re-read what the winner stored and fall
                    // through — if it is our own hash we share the slot.
                    Err(actual) => observed = actual,
                }
            }
            if observed == hash {
                Self::apply(slot, bytes);
                return Insert::Recorded;
            }
            idx = (idx + 1) & SLOT_MASK;
        }
        Insert::Full
    }

    /// Fold one access into an already-claimed slot.
    fn apply(slot: &Slot, bytes: Option<u64>) {
        // Saturating rather than wrapping: a window that somehow recorded 2^32
        // accesses to one partition must not wrap back into the `1` bucket.
        let mut cur = slot.count.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_add(1);
            match slot
                .count
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        match bytes {
            // The working set is defined over DISTINCT partitions, so an entry
            // retains the MAXIMUM weight observed, never a running sum: ten
            // accesses to one partition contribute its size once.
            Some(b) => {
                let mut cur = slot.bytes.load(Ordering::Relaxed);
                while b > cur {
                    match slot.bytes.compare_exchange_weak(
                        cur,
                        b,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(actual) => cur = actual,
                    }
                }
            }
            // Sticky for the rest of the window: once an access to this partition
            // could not be priced, the partition's bytes are not trustworthy and it
            // is reported under `size_source = unavailable` with zero bytes.
            None => {
                slot.flags
                    .fetch_or(FLAG_SIZE_UNAVAILABLE, Ordering::Relaxed);
            }
        }
    }

    /// Visit every live entry. Requires `&mut` (window close holds the write lock).
    pub(super) fn for_each_entry(&mut self, mut f: impl FnMut(Entry)) {
        for slot in self.slots.iter() {
            if slot.key_hash.load(Ordering::Relaxed) == 0 {
                continue;
            }
            f(Entry {
                count: slot.count.load(Ordering::Relaxed),
                bytes: slot.bytes.load(Ordering::Relaxed),
                size_unavailable: slot.flags.load(Ordering::Relaxed) & FLAG_SIZE_UNAVAILABLE != 0,
            });
        }
    }

    /// Empty the table for the next tumbling window.
    pub(super) fn reset(&mut self) {
        for slot in self.slots.iter() {
            slot.clear();
        }
        self.occupancy.store(0, Ordering::Relaxed);
    }

    /// Drop every entry that is not admitted at `prefix_bits`, restoring the
    /// probe-chain invariant in place. Returns the new occupancy.
    ///
    /// Requires `&mut` (the recorder takes the WRITE lock for a downsample), so no
    /// concurrent recorder can observe a half-shifted chain.
    pub(super) fn downsample(&mut self, prefix_bits: u32) -> usize {
        let mut i = 0usize;
        while i < SLOTS {
            let h = self.slots[i].key_hash.load(Ordering::Relaxed);
            if h != 0 && !admitted(h, prefix_bits) {
                self.delete_at(i);
                // Do NOT advance: the backward shift may have moved another entry
                // into slot `i`, and that one may also need dropping.
                continue;
            }
            i += 1;
        }
        self.occupancy()
    }

    /// Backward-shift deletion for linear probing (Knuth 6.4 algorithm R).
    ///
    /// Blanking a slot outright would break every probe chain that passes through
    /// it — later members would become unreachable and a subsequent access to one
    /// of them would claim a SECOND slot, splitting one partition's count into two
    /// lower-repeat entries and silently understating concentration. Shifting the
    /// displaced members back into the hole keeps every survivor findable at its
    /// exact count, with no auxiliary allocation.
    fn delete_at(&mut self, at: usize) {
        self.slots[at].clear();
        self.occupancy.fetch_sub(1, Ordering::Relaxed);

        let mut hole = at;
        let mut probe = (at + 1) & SLOT_MASK;
        // The loop terminates at the first empty slot; a full table is impossible
        // here because we just created a hole.
        while self.slots[probe].key_hash.load(Ordering::Relaxed) != 0 {
            let h = self.slots[probe].key_hash.load(Ordering::Relaxed);
            let home = (h as usize) & SLOT_MASK;
            // Move the entry back only when its home does NOT lie strictly inside
            // the cyclic window `(hole, probe]` — otherwise moving it to `hole`
            // would place it BEFORE its own home and it could never be found.
            if !cyclic_in_exclusive_start(hole, probe, home) {
                self.move_slot(probe, hole);
                hole = probe;
            }
            probe = (probe + 1) & SLOT_MASK;
            if probe == hole {
                break;
            }
        }
    }

    fn move_slot(&mut self, from: usize, to: usize) {
        let (h, b, c, f) = {
            let s = &self.slots[from];
            (
                s.key_hash.load(Ordering::Relaxed),
                s.bytes.load(Ordering::Relaxed),
                s.count.load(Ordering::Relaxed),
                s.flags.load(Ordering::Relaxed),
            )
        };
        let dst = &self.slots[to];
        dst.bytes.store(b, Ordering::Relaxed);
        dst.count.store(c, Ordering::Relaxed);
        dst.flags.store(f, Ordering::Relaxed);
        dst.key_hash.store(h, Ordering::Relaxed);
        self.slots[from].clear();
    }
}

/// Is `x` inside the cyclic range `(start, end]`?
fn cyclic_in_exclusive_start(start: usize, end: usize, x: usize) -> bool {
    if start < end {
        x > start && x <= end
    } else {
        x > start || x <= end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footprint_is_exactly_three_mib() {
        assert_eq!(TABLE_BYTES, 3 * 1024 * 1024);
        assert_eq!(std::mem::size_of::<Slot>(), 24);
        let t = Table::new();
        assert_eq!(t.footprint_bytes(), 3 * 1024 * 1024);
    }

    #[test]
    fn admission_predicate_is_monotone_in_prefix_width() {
        // A key admitted at width k must be admitted at every smaller width —
        // that is what keeps a survivor's already-accumulated count exact.
        for hash in [1u64, 0x0000_0000_dead_beef, u64::MAX, 0x7fff_ffff_ffff_ffff] {
            let mut still = true;
            for k in 0..=20u32 {
                let a = admitted(hash, k);
                if !a {
                    still = false;
                }
                if !still {
                    assert!(!a, "admission must be monotone (hash={hash:#x}, k={k})");
                }
            }
        }
    }

    #[test]
    fn downsample_keeps_every_survivor_findable_at_its_exact_count() {
        // The backward-shift deletion regression: after a downsample every
        // surviving key must still be reachable on its probe chain, so a
        // subsequent access folds into the SAME slot rather than claiming a
        // second one (which would split one partition into two lower-repeat
        // entries and understate concentration).
        let mut table = Table::new();
        let n = 40_000u64;
        for i in 0..n {
            let h = hash_key(&i.to_le_bytes());
            assert_eq!(table.record(h, 0, Some(10)), Insert::Recorded);
        }
        let before = table.occupancy();
        assert_eq!(before, n as usize);

        let survivors = table.downsample(1);
        assert!(survivors < before, "a downsample must drop entries");

        // Every admitted key folds into its existing slot: occupancy is unchanged
        // and each survivor's count becomes exactly 2.
        for i in 0..n {
            let h = hash_key(&i.to_le_bytes());
            match table.record(h, 1, Some(10)) {
                Insert::Recorded => {}
                Insert::NotAdmitted => assert!(!admitted(h, 1)),
                Insert::Full => panic!("table must not be full at {survivors} entries"),
            }
        }
        assert_eq!(
            table.occupancy(),
            survivors,
            "a second pass over the same keys must claim no new slots"
        );
        let mut twos = 0usize;
        let mut others = 0usize;
        table.for_each_entry(|e| if e.count == 2 { twos += 1 } else { others += 1 });
        assert_eq!(others, 0, "every survivor must have folded, not split");
        assert_eq!(twos, survivors);
    }

    #[test]
    fn bytes_take_the_maximum_not_the_sum() {
        let table = Table::new();
        let h = hash_key(b"partition-a");
        for _ in 0..10 {
            assert_eq!(table.record(h, 0, Some(4_096)), Insert::Recorded);
        }
        let mut table = table;
        let mut seen = Vec::new();
        table.for_each_entry(|e| seen.push(e));
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].count, 10);
        assert_eq!(seen[0].bytes, 4_096, "distinct-partition bytes, not a sum");
        assert!(!seen[0].size_unavailable);
    }

    #[test]
    fn an_unpriced_access_makes_the_entry_sticky_unavailable() {
        let table = Table::new();
        let h = hash_key(b"partition-b");
        assert_eq!(table.record(h, 0, Some(8_192)), Insert::Recorded);
        assert_eq!(table.record(h, 0, None), Insert::Recorded);
        assert_eq!(table.record(h, 0, Some(8_192)), Insert::Recorded);
        let mut table = table;
        let mut seen = Vec::new();
        table.for_each_entry(|e| seen.push(e));
        assert_eq!(seen.len(), 1);
        assert!(
            seen[0].size_unavailable,
            "unavailable must be sticky for the window"
        );
    }

    #[test]
    fn reset_empties_the_table_without_changing_its_footprint() {
        let mut table = Table::new();
        for i in 0..1_000u64 {
            table.record(hash_key(&i.to_le_bytes()), 0, Some(1));
        }
        assert_eq!(table.occupancy(), 1_000);
        table.reset();
        assert_eq!(table.occupancy(), 0);
        let mut n = 0;
        table.for_each_entry(|_| n += 1);
        assert_eq!(n, 0);
        assert_eq!(table.footprint_bytes(), TABLE_BYTES);
    }

    #[test]
    fn cyclic_window_membership() {
        assert!(cyclic_in_exclusive_start(2, 5, 3));
        assert!(cyclic_in_exclusive_start(2, 5, 5));
        assert!(!cyclic_in_exclusive_start(2, 5, 2));
        assert!(!cyclic_in_exclusive_start(2, 5, 6));
        // Wrapped window.
        assert!(cyclic_in_exclusive_start(SLOTS - 2, 1, SLOTS - 1));
        assert!(cyclic_in_exclusive_start(SLOTS - 2, 1, 0));
        assert!(!cyclic_in_exclusive_start(SLOTS - 2, 1, 2));
    }
}
