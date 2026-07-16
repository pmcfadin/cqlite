//! Suppression allowlist (design.md §C).
//!
//! Findings are suppressible ONLY through a single committed TOML allowlist.
//! Every entry carries:
//!   * `file` + `fn` + `fingerprint` — the content fingerprint of the allowed
//!     site (a line number would drift; the fingerprint is reformat-stable and
//!     changes when the code changes, so a real new materialization re-fires),
//!   * a non-empty `issue =` reference,
//!   * a non-empty `justification =` string,
//!   * an optional `expiry =` "YYYY-MM-DD" (fails when present and past).
//!
//! An entry whose fingerprint matches no current in-scope finding is *orphaned*
//! and FAILS — the allowlist cannot silently rot.

use std::collections::HashSet;
use std::path::Path;

use super::rule::Finding;

#[derive(Debug, Clone)]
pub struct AllowEntry {
    pub file: String,
    pub function: String,
    pub fingerprint: String,
    pub issue: String,
    pub justification: String,
    pub expiry: Option<String>,
}

#[derive(Debug, Default)]
pub struct Allowlist {
    pub entries: Vec<AllowEntry>,
}

/// A reason an allowlist entry (not a source finding) fails the audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllowlistProblem {
    /// Missing/empty `issue` or `justification`, or malformed `expiry`.
    Malformed { entry: String, reason: String },
    /// `expiry` present and in the past.
    Expired { entry: String, expiry: String },
    /// Fingerprint matches no current in-scope finding.
    Orphaned { entry: String },
}

impl AllowlistProblem {
    pub fn describe(&self) -> String {
        match self {
            AllowlistProblem::Malformed { entry, reason } => {
                format!("malformed allowlist entry [{entry}]: {reason}")
            }
            AllowlistProblem::Expired { entry, expiry } => {
                format!("expired allowlist entry [{entry}]: expiry {expiry} has passed")
            }
            AllowlistProblem::Orphaned { entry } => {
                format!(
                    "orphaned allowlist entry [{entry}]: fingerprint matches no in-scope source"
                )
            }
        }
    }
}

impl Allowlist {
    /// Load from a TOML file. A missing file is an empty allowlist (valid — the
    /// audit simply has no suppressions). A present-but-unparseable file is an
    /// error the caller surfaces.
    pub fn load(path: &Path) -> Result<Allowlist, String> {
        if !path.exists() {
            return Ok(Allowlist::default());
        }
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Allowlist, String> {
        let value: toml::Value =
            toml::from_str(text).map_err(|e| format!("parsing allowlist TOML: {e}"))?;
        let mut entries = Vec::new();
        if let Some(arr) = value.get("allow").and_then(|v| v.as_array()) {
            for item in arr {
                entries.push(AllowEntry {
                    file: str_field(item, "file"),
                    function: str_field(item, "fn"),
                    fingerprint: str_field(item, "fingerprint"),
                    issue: str_field(item, "issue"),
                    justification: str_field(item, "justification"),
                    expiry: item
                        .get("expiry")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                });
            }
        }
        Ok(Allowlist { entries })
    }

    /// Validate every entry structurally: non-empty issue/justification and a
    /// well-formed, non-past expiry (if present). `today` is "YYYY-MM-DD"
    /// (injected so tests are not wall-clock-racy).
    pub fn structural_problems(&self, today: &str) -> Vec<AllowlistProblem> {
        let mut problems = Vec::new();
        for entry in &self.entries {
            let label = entry_label(entry);
            if entry.issue.trim().is_empty() {
                problems.push(AllowlistProblem::Malformed {
                    entry: label.clone(),
                    reason: "empty or missing `issue`".to_string(),
                });
            }
            if entry.justification.trim().is_empty() {
                problems.push(AllowlistProblem::Malformed {
                    entry: label.clone(),
                    reason: "empty or missing `justification`".to_string(),
                });
            }
            if entry.fingerprint.trim().is_empty() {
                problems.push(AllowlistProblem::Malformed {
                    entry: label.clone(),
                    reason: "empty or missing `fingerprint`".to_string(),
                });
            }
            if let Some(expiry) = &entry.expiry {
                match validate_date(expiry) {
                    Err(reason) => problems.push(AllowlistProblem::Malformed {
                        entry: label.clone(),
                        reason,
                    }),
                    Ok(()) => {
                        // Lexicographic compare is correct for zero-padded ISO
                        // dates: past strictly before today.
                        if expiry.as_str() < today {
                            problems.push(AllowlistProblem::Expired {
                                entry: label.clone(),
                                expiry: expiry.clone(),
                            });
                        }
                    }
                }
            }
        }
        problems
    }

    /// Partition `findings` into (unsuppressed, orphaned-entry-problems).
    ///
    /// A finding is suppressed when some entry matches its `(file, fn,
    /// fingerprint)`. Any entry matching no finding is orphaned.
    pub fn apply<'f>(&self, findings: &'f [Finding]) -> (Vec<&'f Finding>, Vec<AllowlistProblem>) {
        let mut matched_entries: HashSet<usize> = HashSet::new();
        let mut remaining = Vec::new();
        for finding in findings {
            let mut suppressed = false;
            for (idx, entry) in self.entries.iter().enumerate() {
                if entry.matches(finding) {
                    matched_entries.insert(idx);
                    suppressed = true;
                }
            }
            if !suppressed {
                remaining.push(finding);
            }
        }
        let mut orphans = Vec::new();
        for (idx, entry) in self.entries.iter().enumerate() {
            if !matched_entries.contains(&idx) {
                orphans.push(AllowlistProblem::Orphaned {
                    entry: entry_label(entry),
                });
            }
        }
        (remaining, orphans)
    }
}

impl AllowEntry {
    fn matches(&self, finding: &Finding) -> bool {
        self.file == finding.file
            && self.function == finding.function
            && self.fingerprint == finding.fingerprint
    }
}

fn entry_label(entry: &AllowEntry) -> String {
    format!("{}::{} {}", entry.file, entry.function, entry.fingerprint)
}

fn str_field(item: &toml::Value, key: &str) -> String {
    item.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// Validate a "YYYY-MM-DD" date shape (not calendar-exact, but rejects garbage).
fn validate_date(s: &str) -> Result<(), String> {
    let parts: Vec<&str> = s.split('-').collect();
    let ok = parts.len() == 3
        && parts[0].len() == 4
        && parts[1].len() == 2
        && parts[2].len() == 2
        && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit()));
    if ok {
        Ok(())
    } else {
        Err(format!("invalid `expiry` date `{s}` (want YYYY-MM-DD)"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(fp: &str) -> Finding {
        Finding {
            rule: "STREAM_RETURNS_VEC",
            file: "cqlite-core/src/query/x.rs".to_string(),
            function: "scan_all".to_string(),
            expr: "it.collect::<Vec<Row>>()".to_string(),
            fingerprint: fp.to_string(),
        }
    }

    fn entry(fp: &str, issue: &str, just: &str, expiry: Option<&str>) -> AllowEntry {
        AllowEntry {
            file: "cqlite-core/src/query/x.rs".to_string(),
            function: "scan_all".to_string(),
            fingerprint: fp.to_string(),
            issue: issue.to_string(),
            justification: just.to_string(),
            expiry: expiry.map(str::to_string),
        }
    }

    #[test]
    fn matched_entry_suppresses_finding() {
        let al = Allowlist {
            entries: vec![entry("f1:abc", "#2012", "bounded small", None)],
        };
        let findings = vec![finding("f1:abc")];
        let (remaining, orphans) = al.apply(&findings);
        assert!(remaining.is_empty());
        assert!(orphans.is_empty());
    }

    #[test]
    fn unmatched_finding_survives_and_entry_orphans() {
        let al = Allowlist {
            entries: vec![entry("f1:stale", "#2012", "bounded", None)],
        };
        let findings = vec![finding("f1:live")];
        let (remaining, orphans) = al.apply(&findings);
        assert_eq!(remaining.len(), 1);
        assert_eq!(orphans.len(), 1);
        assert!(matches!(orphans[0], AllowlistProblem::Orphaned { .. }));
    }

    #[test]
    fn missing_issue_or_justification_is_malformed() {
        let al = Allowlist {
            entries: vec![
                entry("f1:a", "", "has just", None),
                entry("f1:b", "#2012", "", None),
            ],
        };
        let problems = al.structural_problems("2026-07-16");
        assert_eq!(problems.len(), 2);
        assert!(problems
            .iter()
            .all(|p| matches!(p, AllowlistProblem::Malformed { .. })));
    }

    #[test]
    fn past_expiry_fails_future_expiry_passes() {
        let al = Allowlist {
            entries: vec![
                entry("f1:a", "#2012", "j", Some("2020-01-01")),
                entry("f1:b", "#2012", "j", Some("2099-01-01")),
            ],
        };
        let problems = al.structural_problems("2026-07-16");
        assert_eq!(problems.len(), 1);
        assert!(matches!(problems[0], AllowlistProblem::Expired { .. }));
    }

    #[test]
    fn malformed_expiry_is_reported() {
        let al = Allowlist {
            entries: vec![entry("f1:a", "#2012", "j", Some("not-a-date"))],
        };
        let problems = al.structural_problems("2026-07-16");
        assert_eq!(problems.len(), 1);
        assert!(matches!(problems[0], AllowlistProblem::Malformed { .. }));
    }

    #[test]
    fn parse_reads_all_fields() {
        let toml_src = r##"
[[allow]]
file = "cqlite-core/src/query/x.rs"
fn = "scan_all"
fingerprint = "f1:abc"
issue = "#2012"
justification = "bounded small buffer"
expiry = "2099-12-31"
"##;
        let al = Allowlist::parse(toml_src).unwrap();
        assert_eq!(al.entries.len(), 1);
        assert_eq!(al.entries[0].fingerprint, "f1:abc");
        assert_eq!(al.entries[0].expiry.as_deref(), Some("2099-12-31"));
    }
}
