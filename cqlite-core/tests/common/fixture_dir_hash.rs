//! Shared test-support: the Rust side of the cross-language `fixture_dir_sha256`
//! contract (issue #1294 Item 1). Both the dataset-gated parity test
//! (`sstable_parity_corruption_verify.rs`) and the fast dataset-independent unit
//! test (`fixture_dir_sha256_crosslang.rs`) include this module via `#[path]` so
//! the hashing algorithm exists in exactly ONE place — a copy would defeat the
//! purpose of pinning Rust output against the bash/python generator.
//!
//! This MUST stay byte-for-byte identical to `fixture_dir_sha256()` in
//! `test-data/scripts/generate-corruption-corpus.sh`.

use std::path::Path;

/// Deterministic hash over an ENTIRE fixture directory, byte-for-byte identical
/// to `fixture_dir_sha256()` in `generate-corruption-corpus.sh` (issue #1294
/// Item 1): sorted regular-file names directly under the dir; for each mix in the
/// NUL-terminated relative name then the 8-byte-big-endian length + file bytes;
/// finally the 8-byte-big-endian file count. This binds the captured verdict to
/// the COMPLETE set of bytes `sstableverify` reads, not just the mutated component.
///
/// Not every including test target exercises this helper, so allow dead code.
#[allow(dead_code)]
pub fn fixture_dir_sha256(dir: &Path) -> String {
    use sha2::{Digest, Sha256};
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        .flatten()
        .filter(|e| e.path().is_file())
        .filter_map(|e| e.file_name().to_str().map(|s| s.to_string()))
        .collect();
    names.sort();
    let mut h = Sha256::new();
    for n in &names {
        h.update(n.as_bytes());
        h.update([0u8]);
        let data = std::fs::read(dir.join(n))
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.join(n).display()));
        h.update((data.len() as u64).to_be_bytes());
        h.update(&data);
    }
    h.update((names.len() as u64).to_be_bytes());
    format!("{:x}", h.finalize())
}
