//! Public-claim lint (issue #1023), split out of `lint.rs` (campsite rule, #1116).

use crate::enums;
use crate::lint::{check_enum, Finding};
use crate::model::Manifest;

/// Validate the public-claim entries (issue #1023): closed `kind`, well-formed
/// ids, non-empty phrase/rationale, `safe` claims must cite real scenario ids,
/// and `blocked` claims should point at a `claim.safe.*` alternative.
pub(crate) fn lint_claims(m: &Manifest, out: &mut Vec<Finding>) {
    let scenario_ids: std::collections::HashSet<&str> =
        m.scenarios.iter().map(|s| s.id.as_str()).collect();
    let safe_ids: std::collections::HashSet<&str> = m
        .claims
        .iter()
        .filter(|c| c.kind == "safe")
        .map(|c| c.id.as_str())
        .collect();

    let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for c in &m.claims {
        *seen.entry(c.id.as_str()).or_insert(0) += 1;
    }

    for c in &m.claims {
        let id = c.id.as_str();
        if seen.get(id).copied().unwrap_or(0) > 1 {
            out.push(Finding::error(id, "claims.id", "duplicate claim id"));
        }
        check_enum(out, id, "claims.kind", &c.kind, enums::CLAIM_KIND);
        let expected_prefix = match c.kind.as_str() {
            "safe" => "claim.safe.",
            "blocked" => "claim.blocked.",
            _ => "claim.",
        };
        if !id.starts_with(expected_prefix) {
            out.push(Finding::error(
                id,
                "claims.id",
                format!("{} claim id must start with `{expected_prefix}`", c.kind),
            ));
        } else if matches!(c.kind.as_str(), "safe" | "blocked") && !valid_claim_id(id) {
            // The prefix is correct but the full id is malformed (empty or
            // non-`[a-z0-9_]` slug). The schema requires
            // `^claim\.(safe|blocked)\.[a-z0-9_]+$`; enforce it here so the CI
            // `lint` gate cannot publish a malformed id.
            out.push(Finding::error(
                id,
                "claims.id",
                format!(
                    "claim id must match `^claim\\.(safe|blocked)\\.[a-z0-9_]+$` \
                     (non-empty lowercase/digit/underscore slug): {id}"
                ),
            ));
        }
        if c.phrase.trim().is_empty() {
            out.push(Finding::error(
                id,
                "claims.phrase",
                "claim phrase is required",
            ));
        }
        if c.rationale.trim().is_empty() {
            out.push(Finding::error(
                id,
                "claims.rationale",
                "claim rationale is required",
            ));
        }
        match c.kind.as_str() {
            "safe" => {
                if c.evidence_scenarios.is_empty() {
                    out.push(Finding::error(
                        id,
                        "claims.evidence_scenarios",
                        "safe claims must cite at least one backing scenario id",
                    ));
                }
                for sid in &c.evidence_scenarios {
                    if !scenario_ids.contains(sid.as_str()) {
                        out.push(Finding::error(
                            id,
                            "claims.evidence_scenarios",
                            format!("references unknown scenario id: {sid}"),
                        ));
                    }
                }
            }
            "blocked" => match &c.safe_alternative {
                Some(alt) if !safe_ids.contains(alt.as_str()) => {
                    out.push(Finding::error(
                        id,
                        "claims.safe_alternative",
                        format!("references unknown claim.safe.* id: {alt}"),
                    ));
                }
                Some(_) => {}
                None => out.push(Finding::error(
                    id,
                    "claims.safe_alternative",
                    "blocked claims must name a claim.safe.* alternative",
                )),
            },
            _ => {}
        }
    }
}

/// True if `id` matches the schema pattern `^claim\.(safe|blocked)\.[a-z0-9_]+$`:
/// a `claim.safe.` / `claim.blocked.` prefix followed by a non-empty slug of
/// `[a-z0-9_]`. Dependency-free char-class check (no `regex` crate) matching the
/// existing [`valid_id`] style. Callers should only invoke this once the prefix
/// is known to be correct; it re-checks the prefix anyway so it is total.
fn valid_claim_id(id: &str) -> bool {
    let slug = id
        .strip_prefix("claim.safe.")
        .or_else(|| id.strip_prefix("claim.blocked."));
    match slug {
        Some(s) => {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        }
        None => false,
    }
}
