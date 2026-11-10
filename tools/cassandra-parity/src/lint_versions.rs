//! Per-fixture Cassandra version lint (#4482), kept out of `lint.rs` (campsite rule, #1116).

use crate::lint::Finding;
use crate::model::Manifest;

/// A scenario's `evidence.cassandra_version` is a PER-FIXTURE declaration of the
/// release that produced its fixtures (#4482): fixtures generated with the primary
/// `cassandra_source.ref` stay valid until regenerated, and regenerated ones may
/// declare any release listed in `cassandra_source.additional_pins`. A version in
/// neither place is a typo or an undeclared release and is rejected.
pub(crate) fn lint_declared_fixture_versions(m: &Manifest, out: &mut Vec<Finding>) {
    let src = &m.cassandra_source;
    let mut declared: Vec<&str> = Vec::new();
    declared.extend(src.git_ref.strip_prefix("cassandra-"));
    for pin in &src.additional_pins {
        declared.extend(pin.git_ref.strip_prefix("cassandra-"));
    }
    for s in &m.scenarios {
        if let Some(v) = s.evidence.cassandra_version.as_deref() {
            if !v.trim().is_empty() && !declared.contains(&v) {
                out.push(Finding::error(
                    &s.id,
                    "evidence.cassandra_version",
                    format!(
                        "evidence.cassandra_version {v:?} is not a release declared by \
                         cassandra_source.ref or cassandra_source.additional_pins \
                         (declared: {declared:?})"
                    ),
                ));
            }
        }
    }
}
