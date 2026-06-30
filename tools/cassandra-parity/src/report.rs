//! Deterministic public parity report generation (issue #978).
//!
//! Renders `docs/reports/cassandra-test-parity.md` from the manifest. Output is
//! stable across runs (scenarios sorted by id, no timestamps) so CI can run
//! `report --check` to fail on a stale checked-in report.

use std::collections::BTreeMap;

use crate::model::{Claim, Manifest, Scenario};

pub fn render(m: &Manifest, manifest_path: &str) -> String {
    let mut scenarios: Vec<&Scenario> = m.scenarios.iter().collect();
    scenarios.sort_by(|a, b| a.id.cmp(&b.id));

    let mut s = String::new();
    let mut line = |text: &str| {
        s.push_str(text);
        s.push('\n');
    };

    line("# Cassandra Test Parity Report");
    line("");
    line(&format!(
        "> Generated from `{manifest_path}` by `cargo run -p cassandra-parity -- report`. Do not edit by hand — edit the manifest and regenerate."
    ));
    line("");
    line(&format!(
        "Cassandra source: [`{r}`]({repo}/tree/{r}) @ `{sha}` (git SHA). Program: parent epic #{p}, reporting epic #{rep}.",
        r = m.cassandra_source.git_ref,
        repo = m.cassandra_source.repo,
        sha = m.cassandra_source.sha,
        p = m.program.parent_epic,
        rep = m.program.reporting_epic,
    ));
    line("");
    line(&format!(
        "Sources: [`{index}`](../../{index}) · [`{assess}`](../../{assess})",
        index = m.cassandra_source.index,
        assess = m.cassandra_source.assessment_report,
    ));
    line("");

    // --- status counts ---
    line("## Status counts");
    line("");
    line("| Status | Scenarios |");
    line("|---|---|");
    for status in ["mirrored", "partial", "planned", "out_of_scope"] {
        let n = scenarios.iter().filter(|x| x.status == status).count();
        line(&format!("| `{status}` | {n} |"));
    }
    line(&format!("| **total** | **{}** |", scenarios.len()));
    line("");

    // --- evidence counts ---
    line("## Evidence counts");
    line("");
    line("_Counts are per scenario; see [Distinct test backing](#distinct-test-backing-dedup) for the deduplicated test view (issue #1228)._");
    line("");
    line("| Evidence | Scenarios |");
    line("|---|---|");
    for ev in [
        "byte_for_byte",
        "canonical_semantic",
        "smoke",
        "partial",
        "out_of_scope",
    ] {
        let n = scenarios.iter().filter(|x| x.evidence.kind == ev).count();
        line(&format!("| `{ev}` | {n} |"));
    }
    line("");

    // --- warnings: P0 backed only by smoke/partial ---
    line("## ⚠️ P0 scenarios with weak evidence");
    line("");
    let weak_p0: Vec<&&Scenario> = scenarios
        .iter()
        .filter(|x| x.priority == "P0" && matches!(x.evidence.kind.as_str(), "smoke" | "partial"))
        .collect();
    if weak_p0.is_empty() {
        line("_None._");
    } else {
        line("These P0 scenarios are backed only by `smoke` or `partial` evidence and must not be cited as proof of byte parity:");
        line("");
        for x in &weak_p0 {
            line(&format!("- `{}` — {} ({})", x.id, x.title, x.evidence.kind));
        }
    }
    line("");

    // --- P0 scenario table ---
    line("## P0 scenarios");
    line("");
    line("| ID | Capability | Status | Evidence | Suite | Risk |");
    line("|---|---|---|---|---|---|");
    for x in scenarios.iter().filter(|x| x.priority == "P0") {
        let suite = x
            .cqlite
            .coverage
            .suite
            .clone()
            .unwrap_or_else(|| "—".into());
        line(&format!(
            "| `{}` | {} | {} | {} | `{}` | {} |",
            x.id, x.capability, x.status, x.evidence.kind, suite, x.risk
        ));
    }
    line("");

    // --- evidence-grouped sections ---
    render_evidence_group(
        &mut s,
        &scenarios,
        "byte_for_byte",
        "Byte-for-byte scenarios",
        true,
    );
    render_evidence_group(
        &mut s,
        &scenarios,
        "canonical_semantic",
        "Canonical-semantic scenarios",
        false,
    );
    render_evidence_group(&mut s, &scenarios, "smoke", "Smoke-only scenarios", false);

    // --- deduplicated test-backing view (issue #1228) ---
    crate::report_dedup::render(&mut s, &scenarios);

    // --- gaps (partial + planned) ---
    let mut line = |text: &str| {
        s.push_str(text);
        s.push('\n');
    };
    line("## Gaps and next steps");
    line("");
    let gappy: Vec<&&Scenario> = scenarios
        .iter()
        .filter(|x| matches!(x.status.as_str(), "partial" | "planned"))
        .collect();
    if gappy.is_empty() {
        line("_None._");
    } else {
        for x in &gappy {
            let gap = x.scope.gap.clone().unwrap_or_else(|| "—".into());
            let next = x.scope.next_step.clone().unwrap_or_else(|| "—".into());
            line(&format!(
                "- `{}` ({}): {} → _{}_",
                x.id, x.status, gap, next
            ));
        }
    }
    line("");

    // --- out-of-scope taxonomy, grouped by category ---
    line("## Out-of-scope taxonomy");
    line("");
    line("_Out of scope does not mean unimportant._ Node behaviors CQLite does not mirror:");
    line("");
    let mut by_cat: BTreeMap<String, Vec<&&Scenario>> = BTreeMap::new();
    for x in scenarios.iter().filter(|x| x.status == "out_of_scope") {
        let cat = x
            .scope
            .out_of_scope_category
            .clone()
            .unwrap_or_else(|| "(uncategorized)".into());
        by_cat.entry(cat).or_default().push(x);
    }
    if by_cat.is_empty() {
        line("_None._");
        line("");
    } else {
        for (cat, items) in &by_cat {
            line(&format!("### `{cat}`"));
            line("");
            for x in items {
                line(&format!("- `{}` — {}", x.id, x.title));
                if let Some(claim) = &x.scope.safe_claim {
                    if !claim.is_empty() {
                        line(&format!("  - Safe wording: {claim}"));
                    }
                }
            }
            line("");
        }
    }

    // --- CI workflow mapping ---
    line("## CI workflow mapping");
    line("");
    line("| Scenario | CI tier | Workflow |");
    line("|---|---|---|");
    for x in &scenarios {
        let wf = x.ci.workflow.clone().unwrap_or_else(|| "—".into());
        line(&format!("| `{}` | {} | {} |", x.id, x.ci.tier, wf));
    }
    line("");

    // --- fixture / reference mapping ---
    line("## Fixture and reference mapping");
    line("");
    line("| Scenario | Storage fmt | References / failure artifacts |");
    line("|---|---|---|");
    for x in &scenarios {
        let fmt = if x.evidence.storage_format_version.is_empty() {
            "—".to_string()
        } else {
            x.evidence.storage_format_version.join(", ")
        };
        let mut refs: Vec<String> = Vec::new();
        for r in x
            .fixtures
            .references
            .iter()
            .chain(x.evidence.reference_paths.iter())
        {
            if !refs.contains(r) {
                refs.push(r.clone());
            }
        }
        let mut cell = if refs.is_empty() {
            "—".to_string()
        } else {
            refs.join("<br>")
        };
        if !x.evidence.failure_artifacts.is_empty() {
            cell.push_str(&format!(
                "<br>_fail:_ {}",
                x.evidence.failure_artifacts.join(", ")
            ));
        }
        line(&format!("| `{}` | {} | {} |", x.id, fmt, cell));
    }
    line("");

    // --- release-safe claim language (manifest-driven, issue #1023) ---
    render_claim_language(&mut s, m);

    s
}

/// Render the release-safe / blocked public-claim language from the manifest's
/// `claims:` section (issue #1023). Safe wordings cite their backing scenarios;
/// blocked phrases name the safe alternative to use instead. Sorted by id for a
/// deterministic, `report --check`-stable render.
fn render_claim_language(s: &mut String, m: &Manifest) {
    let mut line = |text: &str| {
        s.push_str(text);
        s.push('\n');
    };
    line("## Release-safe claim language");
    line("");
    line(
        "Public/release-facing parity claims are enforced by the claim-scan lint. \
         Safe wordings below are manifest-backed; the blocked phrases are unqualified \
         over-claims rejected unless explicitly scoped as a counter-example.",
    );
    line("");

    let mut safe: Vec<&Claim> = m.claims.iter().filter(|c| c.kind == "safe").collect();
    safe.sort_by(|a, b| a.id.cmp(&b.id));
    let mut blocked: Vec<&Claim> = m.claims.iter().filter(|c| c.kind == "blocked").collect();
    blocked.sort_by(|a, b| a.id.cmp(&b.id));

    line("### Safe wordings");
    line("");
    if safe.is_empty() {
        line("_None._");
    } else {
        for c in &safe {
            line(&format!("- **{}** — {}", c.id, c.phrase.trim()));
            line(&format!("  - Why safe: {}", c.rationale.trim()));
            if !c.evidence_scenarios.is_empty() {
                line(&format!(
                    "  - Backed by: {}",
                    c.evidence_scenarios
                        .iter()
                        .map(|e| format!("`{e}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
    }
    line("");

    line("### Blocked phrases (rejected unless explicitly scoped)");
    line("");
    if blocked.is_empty() {
        line("_None._");
    } else {
        for c in &blocked {
            line(&format!("- **{}** — \"{}\"", c.id, c.phrase.trim()));
            line(&format!("  - Why blocked: {}", c.rationale.trim()));
            if let Some(alt) = &c.safe_alternative {
                line(&format!("  - Use instead: `{alt}`"));
            }
        }
    }
    line("");
}

fn render_evidence_group(
    s: &mut String,
    scenarios: &[&Scenario],
    kind: &str,
    title: &str,
    byte: bool,
) {
    let mut line = |text: &str| {
        s.push_str(text);
        s.push('\n');
    };
    line(&format!("## {title}"));
    line("");
    let items: Vec<&&Scenario> = scenarios
        .iter()
        .filter(|x| x.evidence.kind == kind)
        .collect();
    if items.is_empty() {
        if byte {
            line("_None yet._ No scenario currently claims byte-for-byte parity; coverage is canonical-semantic or weaker. This is intentional and honest — see the assessment report.");
        } else {
            line("_None._");
        }
        line("");
        return;
    }
    for x in &items {
        // A `planned` scenario carries an evidence *type* but no evidence yet;
        // mark it so the section never reads as if the parity exists today
        // (issue #995 — wide_partition_corpus is planned canonical_semantic).
        let planned = if x.status == "planned" {
            " _(planned — no evidence yet)_"
        } else {
            ""
        };
        line(&format!("- `{}` — {}{planned}", x.id, x.title));
        if let Some(norm) = &x.evidence.normalization {
            if !norm.is_empty() {
                line(&format!("  - Normalization: {norm}"));
            }
        }
        // delta_scan scenarios are JSONL semantic parity only; surface that
        // byte-for-byte Data.db backing is still a follow-up so the report does
        // not read as byte parity (issue #995, AC3/AC7). Skip the note for
        // planned scenarios, which have no evidence to qualify.
        if x.capability == "delta_scan" && x.status != "planned" {
            line("  - Byte-for-byte: not yet — needs Data.db backing (follow-up under epic #969).");
        }
    }
    line("");
}
