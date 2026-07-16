//! `xtask` library surface (issue #2012).
//!
//! The audit logic lives here (not in `main.rs`) so it is reachable from the
//! crate-external self-test fixtures under `xtask/tests/` and from any future
//! subcommand. The binary (`main.rs`) is a thin arg-parse shell over `oom_audit`.

pub mod oom_audit;
