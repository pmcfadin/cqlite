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
