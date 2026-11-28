//! #4671 finding 3: trusted metadata does not retain `Statistics.db` bytes. With many generations
//! listed, none of the (up to 64 MiB) read buffers stays alive after its parse: a counting global
//! allocator watches the live heap while the shim hands the loader buffers with a large capacity.
//!
//! Own test binary: `#[global_allocator]` is per binary, and a single test keeps the counter free
//! of other threads' allocations.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/generation_fixtures.rs"]
mod generation_fixtures;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use cqlite_core::storage::sstable::generation::{list_generations, Scope, TocPolicy};
use generation_fixtures::*;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size(), Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        LIVE.fetch_add(new, Ordering::Relaxed);
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        System.realloc(p, l, new)
    }
}

#[global_allocator]
static A: Counting = Counting;

const GENERATIONS: usize = 12;
const BUFFER: usize = 8 * 1024 * 1024;

#[test]
fn no_statistics_buffer_outlives_its_parse() {
    let src = fixture_table("snap_backup", "snap_backup");
    let real = std::fs::read(src.join("nb-1-big-Statistics.db")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    for i in 1..=GENERATIONS {
        for component in ["Data.db", "TOC.txt", "Statistics.db"] {
            touch(&dir.path().join(format!("nb-{i}-big-{component}")));
        }
    }
    let listing = list_generations(dir.path(), &Scope::Live, TocPolicy::Accept).unwrap();
    assert_eq!(listing.live.len(), GENERATIONS);

    let before = LIVE.load(Ordering::Relaxed);
    let shim = |_: &std::path::Path| {
        let mut buffer = Vec::with_capacity(BUFFER);
        buffer.extend_from_slice(&real);
        Ok(buffer)
    };
    for generation in &listing.live {
        generation.trusted_metadata_via(&shim).expect("metadata");
        generation.statistics_facts_via(&shim).expect("facts");
    }
    let retained = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    assert!(
        retained < BUFFER,
        "{retained} bytes retained after loading {GENERATIONS} generations (a buffer is {BUFFER})"
    );
}
