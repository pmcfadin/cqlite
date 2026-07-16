//! `xtask` — CQLite developer-tooling entry point (issue #2012).
//!
//! Currently hosts a single subcommand, `oom-audit`, a `syn`-based AST audit
//! for the "never materialize an unbounded read" memory-safety invariant. See
//! `oom_audit` for the rule and allowlist machinery.
//!
//! Usage:
//!   cargo run -p xtask -- oom-audit            # report-only, always exits 0
//!   cargo run -p xtask -- oom-audit --enforce  # non-zero on any failing finding

use std::process::ExitCode;

use xtask::oom_audit;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("oom-audit") => run_oom_audit(&args[1..]),
        Some(other) => {
            eprintln!("xtask: unknown subcommand `{other}`");
            print_usage();
            ExitCode::from(2)
        }
        None => {
            print_usage();
            ExitCode::from(2)
        }
    }
}

fn run_oom_audit(rest: &[String]) -> ExitCode {
    let mut enforce = false;
    for arg in rest {
        match arg.as_str() {
            "--enforce" => enforce = true,
            other => {
                eprintln!("oom-audit: unknown argument `{other}`");
                print_usage();
                return ExitCode::from(2);
            }
        }
    }

    let repo_root = match oom_audit::repo_root() {
        Ok(root) => root,
        Err(e) => {
            eprintln!("oom-audit: could not locate workspace root: {e}");
            return ExitCode::from(2);
        }
    };

    match oom_audit::run(&repo_root, enforce) {
        Ok(outcome) => outcome.exit_code(enforce),
        Err(e) => {
            eprintln!("oom-audit: {e}");
            ExitCode::from(2)
        }
    }
}

fn print_usage() {
    eprintln!("usage: cargo run -p xtask -- oom-audit [--enforce]");
}
