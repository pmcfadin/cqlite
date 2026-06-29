//! Deduplicated evidence tally for the parity report (issue #1228).
//!
//! The headline status/evidence tables count *scenarios*, and one backing test
//! can map to many scenario ids (e.g. `issue_1000_verifier.rs` backs 18
//! `cass.verify.*` ids, `scan_delta_parity_test.rs` backs 10 `cass.delta_scan.*`
//! ids). Counting scenarios alone overstates how much *distinct* test work backs
//! the program. This module renders an explicit dedup view: the number of
//! distinct backing tests, and a fan-out table (test -> scenario count) so the
//! public report cannot be read as "N independent proofs" when it is really one
//! test exercised across N ids.

use std::collections::BTreeMap;

use crate::model::Scenario;

/// Build a deterministic map of backing-test target -> the scenario ids that
/// name it in `cqlite.coverage.tests`. A scenario with several tests contributes
/// to each. Sorted by test path for a stable, `report --check`-safe render.
pub fn test_fanout<'a>(scenarios: &[&'a Scenario]) -> BTreeMap<String, Vec<&'a str>> {
    let mut map: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for s in scenarios {
        for t in &s.cqlite.coverage.tests {
            map.entry(t.clone()).or_default().push(s.id.as_str());
        }
    }
    for ids in map.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }
    map
}

/// Render the "Distinct test backing" section: total distinct backing tests vs
/// total scenarios-with-tests, plus the multi-id fan-out table (tests that back
/// more than one scenario, where the scenario count would otherwise inflate the
/// headline tally).
pub fn render(out: &mut String, scenarios: &[&Scenario]) {
    let mut line = |text: &str| {
        out.push_str(text);
        out.push('\n');
    };

    let fanout = test_fanout(scenarios);
    let distinct_tests = fanout.len();
    let scenarios_with_tests = scenarios
        .iter()
        .filter(|s| !s.cqlite.coverage.tests.is_empty())
        .count();

    line("## Distinct test backing (dedup)");
    line("");
    line(
        "Scenario counts above can overstate distinct proof: one backing test may \
         exercise many scenario ids. The dedup view below counts unique test targets \
         so the program is not read as more independent tests than exist (issue #1228).",
    );
    line("");
    line(&format!(
        "- Distinct backing tests: **{distinct_tests}** across **{scenarios_with_tests}** \
         scenarios that name a test."
    ));
    line("");

    // Fan-out table: only tests backing >1 scenario (the inflation cases).
    let mut multi: Vec<(&String, &Vec<&str>)> =
        fanout.iter().filter(|(_, ids)| ids.len() > 1).collect();
    multi.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(b.0)));

    line("### Tests backing more than one scenario");
    line("");
    if multi.is_empty() {
        line("_None — every backing test maps to exactly one scenario._");
    } else {
        line("| Backing test | Scenarios backed |");
        line("|---|---|");
        for (test, ids) in &multi {
            line(&format!("| `{}` | {} |", test, ids.len()));
        }
    }
    line("");
}
