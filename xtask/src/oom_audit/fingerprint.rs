//! Content fingerprint for allowlist entries (design.md §C).
//!
//! The fingerprint is a stable hash of the `quote`-normalized token stream of
//! the offending expression. Normalizing through a `proc_macro2::TokenStream`
//! collapses all whitespace/formatting differences (so a reformat does not churn
//! the allowlist) while still changing whenever the tokens themselves change (so
//! a real new materialization at a previously-allowed site re-fires).
//!
//! We deliberately use a small in-tree FNV-1a-64 hash rather than pulling in a
//! hashing crate: the spec (#2012) caps this crate's dependencies at
//! syn/quote/walkdir/toml. FNV-1a is deterministic and stable across Rust
//! versions and platforms (unlike `std`'s `DefaultHasher`, whose SipHash keys
//! are not contractually stable), which is exactly what a committed fingerprint
//! requires.

use proc_macro2::TokenStream;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Fingerprint a token stream: normalize (via `to_string`, which canonicalizes
/// spacing) then FNV-1a-64 the bytes. Prefixed `f1:` to name the algorithm so a
/// future scheme change is distinguishable in committed allowlists.
pub fn fingerprint_tokens(tokens: &TokenStream) -> String {
    let normalized = tokens.to_string();
    let mut hash = FNV_OFFSET;
    for byte in normalized.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("f1:{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn whitespace_and_formatting_do_not_change_fingerprint() {
        // Same tokens, different source spacing -> identical fingerprint.
        let a: TokenStream = "reader . rows ( ) . collect ::< Vec < DataRow >> ( )"
            .parse()
            .unwrap();
        let b: TokenStream = "reader.rows().collect::<Vec<DataRow>>()".parse().unwrap();
        assert_eq!(fingerprint_tokens(&a), fingerprint_tokens(&b));
    }

    #[test]
    fn different_tokens_change_fingerprint() {
        let a = quote! { reader.rows().collect::<Vec<DataRow>>() };
        let b = quote! { reader.cells().collect::<Vec<DataCell>>() };
        assert_ne!(fingerprint_tokens(&a), fingerprint_tokens(&b));
    }

    #[test]
    fn fingerprint_is_prefixed_and_stable() {
        let a = quote! { reader.rows().collect::<Vec<DataRow>>() };
        assert!(fingerprint_tokens(&a).starts_with("f1:"));
        // Deterministic across calls.
        assert_eq!(fingerprint_tokens(&a), fingerprint_tokens(&a));
    }
}
