//! Fast, dataset-independent pin of the cross-language `fixture_dir_sha256`
//! contract (issue #1294 Item 1, roborev Finding 2).
//!
//! The corruption-verify scheme depends on the Rust `fixture_dir_sha256`
//! (`common/fixture_dir_hash.rs`, shared with `sstable_parity_corruption_verify.rs`)
//! and the bash/python `fixture_dir_sha256` in
//! `test-data/scripts/generate-corruption-corpus.sh` producing BYTE-IDENTICAL
//! hashes for the same directory contents. The dataset-gated integration test
//! only checks this when the (large) corruption corpus is present; this test pins
//! the same contract with a tiny synthetic directory so it runs always and fast,
//! with no `CQLITE_DATASETS_ROOT` / dataset dependency.
//!
//! The GOLDEN below is the bash/python generator's output over the identical
//! synthetic directory built by `build_fixture()`. It pins the Rust hash to the
//! bash-derived value, exactly as the integration test pins Rust vs the
//! `verdict_captured_for_dir_sha256` the generator wrote.
//!
//! REGENERATE THE GOLDEN (re-run after any change to the bash `fixture_dir_sha256`):
//! build the same files, then run the generator's exact python block over the dir:
//!
//! ```sh
//! FIX=$(mktemp -d)
//! python3 - "$FIX" <<'PY'
//! import sys, os
//! d = sys.argv[1]
//! open(os.path.join(d,"10-Data.db"),"wb").write(bytes([0xDE,0xAD,0xBE,0xEF,0x00,0x01]))
//! open(os.path.join(d,"2-Data.db"),"wb").write(b"hello")
//! open(os.path.join(d,"Index.db"),"wb").write(bytes([0,0,0]))
//! open(os.path.join(d,"empty.db"),"wb").write(b"")
//! open(os.path.join(d,"a.db"),"wb").write(bytes(range(256)))
//! PY
//! # the fixture_dir_sha256 python from generate-corruption-corpus.sh:
//! python3 - "$FIX" <<'PY'
//! import sys, os, hashlib
//! d = sys.argv[1]
//! names = sorted(n for n in os.listdir(d) if os.path.isfile(os.path.join(d, n)))
//! h = hashlib.sha256()
//! for n in names:
//!     h.update(n.encode("utf-8")); h.update(b"\x00")
//!     data = open(os.path.join(d, n), "rb").read()
//!     h.update(len(data).to_bytes(8, "big")); h.update(data)
//! h.update(len(names).to_bytes(8, "big"))
//! print(h.hexdigest())
//! PY
//! ```

use std::path::Path;

// Shared Rust hashing algorithm (the ONE copy — see the module doc).
#[path = "common/fixture_dir_hash.rs"]
mod fixture_dir_hash;
use fixture_dir_hash::fixture_dir_sha256;

/// The bash/python generator's `fixture_dir_sha256` output over the directory
/// `build_fixture()` produces. Derived by the command in the module docs above.
const GOLDEN: &str = "5e6a02569b5ddb8ae5f3d319e9245d0c4cddf2a4a0d704ce39cbe0f45466aa38";

/// Build a deterministic synthetic fixture directory exercising the edge cases:
/// several files whose names sort non-trivially ('1'/'2' before uppercase 'I'
/// before lowercase 'a'), an empty file, and a file with all 256 byte values.
fn build_fixture(dir: &Path) {
    std::fs::write(
        dir.join("10-Data.db"),
        [0xDEu8, 0xAD, 0xBE, 0xEF, 0x00, 0x01],
    )
    .expect("write 10-Data.db");
    std::fs::write(dir.join("2-Data.db"), b"hello").expect("write 2-Data.db");
    std::fs::write(dir.join("Index.db"), [0u8, 0, 0]).expect("write Index.db");
    std::fs::write(dir.join("empty.db"), b"").expect("write empty.db");
    let all_bytes: Vec<u8> = (0u16..256).map(|b| b as u8).collect();
    std::fs::write(dir.join("a.db"), &all_bytes).expect("write a.db");
}

#[test]
fn rust_fixture_dir_sha256_matches_bash_generator_golden() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    build_fixture(tmp.path());

    let got = fixture_dir_sha256(tmp.path());
    assert_eq!(
        got, GOLDEN,
        "Rust fixture_dir_sha256 diverged from the bash/python generator golden.\n\
         If the bash `fixture_dir_sha256` in generate-corruption-corpus.sh changed, \
         regenerate GOLDEN with the command in this file's module docs and update the \
         Rust helper (common/fixture_dir_hash.rs) to match — the cross-language \
         corruption-verify contract (issue #1294 Item 1) requires byte-identical hashes."
    );
}
