//! Rendering of `cassandra-parity lint` findings (kept out of `main.rs`, campsite rule #1116).

use std::process::ExitCode;

use crate::lint::{Finding, Level};
use crate::model::Manifest;

/// Print the findings and the verdict line. Partial-claim reports (#4492) collapse into ONE
/// summary line; the per-scenario detail is printed only under `--verbose`, so the real
/// warnings are not drowned out.
pub fn print_and_exit(findings: Vec<Finding>, m: &Manifest, verbose: bool) -> ExitCode {
    let (partial, findings): (Vec<_>, Vec<_>) = findings
        .into_iter()
        .partition(|f| f.level == Level::Warn && f.message.starts_with("partial claim:"));
    let errors = findings.iter().filter(|f| f.level == Level::Error).count();
    let warns = findings.iter().filter(|f| f.level == Level::Warn).count();
    if !partial.is_empty() {
        let entries: usize = m
            .cassandra_source
            .carried_over
            .iter()
            .map(|c| c.tables.len())
            .sum();
        println!(
            "carried-over: {} scenarios declare a release other than the one that wrote the \
             carried-over tables they touch ({entries} carried-over table entries declared); \
             see --verbose",
            partial.len()
        );
        if verbose {
            for f in &partial {
                println!("info [{}] {}: {}", f.id, f.field, f.message);
            }
        }
    }
    for f in &findings {
        let tag = match f.level {
            Level::Error => "ERROR",
            Level::Warn => "warn",
        };
        println!("{tag} [{}] {}: {}", f.id, f.field, f.message);
    }
    if errors == 0 {
        println!(
            "lint: OK — {} scenarios, 0 errors, {warns} warnings",
            m.scenarios.len()
        );
        ExitCode::SUCCESS
    } else {
        eprintln!("lint: FAILED — {errors} errors, {warns} warnings");
        ExitCode::FAILURE
    }
}
