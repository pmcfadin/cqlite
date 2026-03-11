# Agent Notes

- If you change `cqlite-core`, run `cargo clippy -p cqlite-core --all-targets --all-features -- -D warnings` before pushing.
- Do not treat a green `cargo test` run as sufficient for `cqlite-core`; a Clippy-clean `cqlite-core` pass is also required.
