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

/// `true` for non-component sidecars that the generator's copy-cleanup step
/// (`find "$dest" -maxdepth 1 -type f \( -name '*.db.jsonl' -o -name '*.db.txt' \
/// -o -name '.DS_Store' -o -name '._*' \) -delete` in
/// `generate-corruption-corpus.sh`, ~line 645-647) already strips before a
/// fixture is committed. MUST mirror that filter and the bash `is_sidecar()`
/// inside `fixture_dir_sha256()` byte-for-byte (issue #1294 roborev Medium
/// finding) — without this a stray `.DS_Store` / AppleDouble `._*` file from a
/// macOS checkout would fold into the hash and trip the unconditional-fatal
/// MODE 1 dir-sha check. Changing this predicate is a prompt to update BOTH the
/// bash `is_sidecar()` and the `find -delete` pattern it mirrors.
fn is_sidecar(name: &str) -> bool {
    name == ".DS_Store"
        || name.starts_with("._")
        || name.ends_with(".db.jsonl")
        || name.ends_with(".db.txt")
}

/// Deterministic hash over an ENTIRE fixture directory, byte-for-byte identical
/// to `fixture_dir_sha256()` in `generate-corruption-corpus.sh` (issue #1294
/// Item 1): sorted regular-file names directly under the dir (excluding the
/// non-component sidecars in `is_sidecar()`); for each mix in the
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
        .filter(|n| !is_sidecar(n))
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
