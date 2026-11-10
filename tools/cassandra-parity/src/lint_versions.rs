//! Per-fixture Cassandra version lint (#4482), kept out of `lint.rs` (campsite rule, #1116).

use crate::lint::Finding;
use crate::model::Manifest;

/// A scenario's `evidence.cassandra_version` / `evidence.cassandra_git_sha` are a
/// PER-FIXTURE declaration of the release that produced its fixtures (#4482): fixtures
/// generated with the primary `cassandra_source` pin stay valid until regenerated, and
/// regenerated ones may declare any pin in `cassandra_source.additional_pins`.
///
/// The version and the sha must belong to the SAME declared pin (roborev job 174): a
/// version from one pin with the sha of another is rejected, so a mismatched sha can
/// never be laundered through a scenario. Either field alone is an error (both are
/// required whenever one is present). A version in no pin is a typo or an undeclared
/// release.
pub(crate) fn lint_declared_fixture_versions(m: &Manifest, out: &mut Vec<Finding>) {
    lint_pin_uniqueness(m, out);
    lint_carried_over(m, out);
    let pins = m.cassandra_source.pins();
    let declared: Vec<&str> = pins.iter().map(|(v, _, _)| v.as_str()).collect();
    for s in &m.scenarios {
        let version = s
            .evidence
            .cassandra_version
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty());
        let sha = s
            .evidence
            .cassandra_git_sha
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty());
        // EITHER field present requires BOTH: a lone sha (or lone version) would skip the
        // same-pin check below and let an arbitrary value stand as a declaration.
        let (version, sha) = match (version, sha) {
            (None, None) => continue,
            (Some(v), Some(s)) => (v, Some(s)),
            (Some(_), None) => {
                out.push(Finding::error(
                    &s.id,
                    "evidence.cassandra_git_sha",
                    "evidence.cassandra_version is set but evidence.cassandra_git_sha is missing; \
                     both are required and must come from the same cassandra_source pin",
                ));
                continue;
            }
            (None, Some(_)) => {
                out.push(Finding::error(
                    &s.id,
                    "evidence.cassandra_version",
                    "evidence.cassandra_git_sha is set but evidence.cassandra_version is missing; \
                     both are required and must come from the same cassandra_source pin",
                ));
                continue;
            }
        };
        let Some((_, _, pin_sha)) = pins.iter().find(|(v, _, _)| v == version) else {
            out.push(Finding::error(
                &s.id,
                "evidence.cassandra_version",
                format!(
                    "evidence.cassandra_version {version:?} is not a release declared by \
                     cassandra_source.ref or cassandra_source.additional_pins \
                     (declared: {declared:?})"
                ),
            ));
            continue;
        };
        if let Some(sha) = sha {
            if sha != *pin_sha {
                out.push(Finding::error(
                    &s.id,
                    "evidence.cassandra_git_sha",
                    format!(
                        "evidence.cassandra_git_sha {sha:?} is not the sha of the pin for \
                         evidence.cassandra_version {version:?} ({pin_sha}); version and sha \
                         must come from the same cassandra_source pin"
                    ),
                ));
            }
        }
    }
}

/// The declared pins (primary plus `additional_pins`) must be distinct releases: a duplicate
/// derived VERSION or a duplicate SHA would let evidence that matches one entry stand for
/// the other (roborev job 180), so each is an error naming both entries.
fn lint_pin_uniqueness(m: &Manifest, out: &mut Vec<Finding>) {
    let pins = m.cassandra_source.pins();
    for (i, (version, git_ref, sha)) in pins.iter().enumerate() {
        for (other_version, other_ref, other_sha) in &pins[i + 1..] {
            if version == other_version {
                out.push(Finding::error(
                    "cassandra_source",
                    "cassandra_source.additional_pins",
                    format!(
                        "pins {git_ref} ({sha}) and {other_ref} ({other_sha}) derive the same \
                         version {version:?}; each declared release must be distinct"
                    ),
                ));
            }
            if sha == other_sha {
                out.push(Finding::error(
                    "cassandra_source",
                    "cassandra_source.additional_pins",
                    format!(
                        "pins {git_ref} and {other_ref} declare the same sha {sha}; each declared \
                         release must be distinct"
                    ),
                ));
            }
        }
    }
}

/// `cassandra_source.carried_over` (#4492) is the per-table provenance truth, so it must be
/// well formed: each entry names a declared pin, tables are `keyspace.table` / `keyspace.*`,
/// and no table is carried over twice. Scenarios that touch carried-over tables while
/// declaring a different release are REPORTED (warning) with the number of such tables, so
/// a scenario's `evidence.cassandra_version` (the release that wrote its NON-carried-over
/// data) is never silently a partial claim.
fn lint_carried_over(m: &Manifest, out: &mut Vec<Finding>) {
    use crate::corpus_audit::refs::{corpus_refs, strip_uuid_suffix, CORPUS_PREFIX};
    let pins = m.cassandra_source.pins();
    let mut seen: Vec<&str> = Vec::new();
    for c in &m.cassandra_source.carried_over {
        if !pins.iter().any(|(_, r, _)| *r == c.pin_ref) {
            out.push(Finding::error(
                "cassandra_source",
                "cassandra_source.carried_over",
                format!("carried_over pin {:?} is not a declared pin", c.pin_ref),
            ));
        }
        if c.tables.is_empty() {
            out.push(Finding::error(
                "cassandra_source",
                "cassandra_source.carried_over",
                format!("carried_over entry for {} lists no tables", c.pin_ref),
            ));
        }
        for t in &c.tables {
            let ok = t
                .split_once('.')
                .is_some_and(|(k, n)| !k.is_empty() && !n.is_empty() && !n.contains('.'));
            if !ok {
                out.push(Finding::error(
                    "cassandra_source",
                    "cassandra_source.carried_over",
                    format!("carried_over table {t:?} must be `keyspace.table` or `keyspace.*`"),
                ));
            }
            if seen.contains(&t.as_str()) {
                out.push(Finding::error(
                    "cassandra_source",
                    "cassandra_source.carried_over",
                    format!("table {t:?} is carried over more than once"),
                ));
            }
            seen.push(t);
        }
    }
    if m.cassandra_source.carried_over.is_empty() {
        return;
    }
    let primary_version = pins[0].0.as_str();
    for s in &m.scenarios {
        let declared = s
            .evidence
            .cassandra_version
            .as_deref()
            .map(str::trim)
            .unwrap_or(primary_version);
        let mut touched: std::collections::BTreeSet<String> = Default::default();
        for r in corpus_refs(s) {
            let Some(under) = r[CORPUS_PREFIX.len()..]
                .trim_end_matches('/')
                .strip_prefix("sstables")
            else {
                continue;
            };
            let parts: Vec<&str> = under.split('/').filter(|p| !p.is_empty()).collect();
            if parts.len() >= 2 {
                let t = format!("{}.{}", parts[0], strip_uuid_suffix(parts[1]));
                if m.cassandra_source.carried_over_pin(&t).is_some() {
                    touched.insert(t);
                }
            } else {
                // keyspace / sstables-root reference: every carried-over table under it.
                for c in &m.cassandra_source.carried_over {
                    for t in &c.tables {
                        if parts
                            .first()
                            .is_none_or(|k| t.split('.').next() == Some(*k))
                        {
                            touched.insert(t.clone());
                        }
                    }
                }
            }
        }
        if touched.is_empty() {
            continue;
        }
        let carried_versions: std::collections::BTreeSet<String> = touched
            .iter()
            .filter_map(|t| m.cassandra_source.carried_over_pin(t))
            .map(|(v, _, _)| v)
            .collect();
        if carried_versions.iter().any(|v| v != declared) {
            out.push(Finding::warn(
                &s.id,
                "evidence.cassandra_version",
                format!(
                    "partial claim: declares {declared} for its regenerated data, but covers {} \
                     carried-over table(s)/pattern(s) written by {:?} (per-table truth: \
                     cassandra_source.carried_over)",
                    touched.len(),
                    carried_versions
                ),
            ));
        }
    }
}
