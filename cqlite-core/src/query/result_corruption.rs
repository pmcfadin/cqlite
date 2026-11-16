//! TEST-ONLY decoded-cell corruption seam (issue #4256).
//!
//! # Why this exists
//!
//! The differential/parity suites each prove their COMPARATOR can fail on a
//! synthetic diff, but none proved the WHOLE suite goes red when the real read
//! path hands back one wrong decoded value. This seam lets a meta-test corrupt
//! exactly one decoded cell of a real `SELECT` result — at the engine's single
//! result chokepoint (`AdvancedQueryEngine::execute`) — and then assert the suite's own
//! assertion fires. A suite that stays green under that corruption is comparing
//! something other than the path's output.
//!
//! # Scope
//!
//! * Compiled ONLY with the `result-corruption-injection` feature (NOT in defaults;
//!   no dependencies). Without it the module, the arming statics and the one call
//!   site in `engine::deadline` (`AdvancedQueryEngine::execute`) do not exist, so every default build, the
//!   shipped CLI/bindings and CI are byte-identical.
//! * Doc-hidden and unarmed by default: with the feature on but nothing armed, the
//!   call site is one uncontended mutex read and rows are untouched.
//! * Corruption replaces the FIRST column's value (by column name, sorted) of the
//!   FIRST row with a value that is guaranteed to differ (`Null`, or `Integer(0xBAD)`
//!   when it already was `Null`), so no suite can mask it by value coincidence.
//! * An arm is keyed by an [`ArmFilter`] on the result's recorded `AccessPath`, so a
//!   point-vs-full meta-test can corrupt ONLY the partition-targeted leg and leave
//!   the full-scan leg honest — the "corrupt one decoded cell on the path under
//!   test" shape. The arm stays live until its guard drops (every matching result is
//!   corrupted), and the process-global state means a test arming it must be
//!   `#[serial]` against every other test in its binary that reads.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::query::access_path::AccessPath;
use crate::query::result::QueryRow;
use crate::types::Value;

/// Which results an arm corrupts, by the access path the result records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmFilter {
    /// Only partition-TARGETED results (`AccessPath::is_targeted`): the point leg.
    TargetedOnly,
    /// Only full-scan results (`AccessPath::is_full_scan`): the full leg.
    FullScanOnly,
    /// Every result.
    Any,
}

impl ArmFilter {
    fn matches(self, path: Option<&AccessPath>) -> bool {
        match (self, path) {
            (ArmFilter::Any, _) => true,
            (ArmFilter::TargetedOnly, Some(p)) => p.is_targeted(),
            (ArmFilter::FullScanOnly, Some(p)) => p.is_full_scan(),
            (_, None) => false,
        }
    }
}

struct Arm {
    id: u64,
    filter: ArmFilter,
    /// Set once the arm has corrupted a result; a fired arm never matches again.
    fired: bool,
}

static ARMS: Mutex<Vec<Arm>> = Mutex::new(Vec::new());
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static CORRUPTED: AtomicU64 = AtomicU64::new(0);

/// RAII arm: corrupts exactly ONE matching, non-empty result, then is spent (and
/// disarmed for good when dropped).
#[must_use = "the arm is disarmed as soon as the guard is dropped"]
pub struct CorruptionArm {
    id: u64,
}

impl Drop for CorruptionArm {
    fn drop(&mut self) {
        ARMS.lock().retain(|a| a.id != self.id);
    }
}

/// Arm the seam for results matching `filter`.
pub fn arm(filter: ArmFilter) -> CorruptionArm {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    ARMS.lock().push(Arm {
        id,
        filter,
        fired: false,
    });
    CorruptionArm { id }
}

/// Number of results corrupted so far in this process: a meta-test reads it to prove
/// the arm actually FIRED (an arm that never fires would make a green suite look
/// like a failed meta-test for the wrong reason, or the reverse).
pub fn corrupted_count() -> u64 {
    CORRUPTED.load(Ordering::Relaxed)
}

/// The executor's call site: corrupt one cell of `rows` if an unfired arm matches `path`.
pub(crate) fn maybe_corrupt(rows: &mut [QueryRow], path: Option<&AccessPath>) {
    if corrupt_with(&ARMS, rows, path) {
        CORRUPTED.fetch_add(1, Ordering::Relaxed);
    }
}

/// Consume the first unfired matching arm in `arms` by corrupting the first cell of
/// `rows`. The arm is marked fired under the same lock hold, and only after a cell was
/// actually corrupted: empty / non-matching results leave every arm live.
fn corrupt_with(arms: &Mutex<Vec<Arm>>, rows: &mut [QueryRow], path: Option<&AccessPath>) -> bool {
    let mut arms = arms.lock();
    let Some(arm) = arms.iter_mut().find(|a| !a.fired && a.filter.matches(path)) else {
        return false;
    };
    let Some(row) = rows.first_mut() else {
        return false;
    };
    let Some(name) = row.values.keys().min().cloned() else {
        return false;
    };
    let Some(value) = row.values.get_mut(&name) else {
        return false;
    };
    *value = if matches!(value, Value::Null) {
        Value::Integer(0xBAD)
    } else {
        Value::Null
    };
    arm.fired = true;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: i32) -> QueryRow {
        let mut r = QueryRow::new(crate::types::RowKey(std::sync::Arc::from(&b"k"[..])));
        r.values
            .insert(std::sync::Arc::from("a"), Value::Integer(v));
        r
    }

    fn arms_with(filter: ArmFilter) -> Mutex<Vec<Arm>> {
        Mutex::new(vec![Arm {
            id: 1,
            filter,
            fired: false,
        }])
    }

    #[test]
    fn an_arm_corrupts_exactly_one_result_and_empty_results_do_not_consume_it() {
        let arms = arms_with(ArmFilter::Any);
        let path = AccessPath::FullScan;

        // An empty matching result must not consume the arm.
        assert!(!corrupt_with(&arms, &mut [], Some(&path)));
        assert!(!arms.lock()[0].fired);

        // Two matching results: exactly the first is corrupted.
        let (mut first, mut second) = ([row(7)], [row(7)]);
        assert!(corrupt_with(&arms, &mut first, Some(&path)));
        assert!(!corrupt_with(&arms, &mut second, Some(&path)));
        let corrupted = [&first[0], &second[0]]
            .iter()
            .filter(|r| !matches!(r.values.get("a"), Some(Value::Integer(7))))
            .count();
        assert_eq!(corrupted, 1, "exactly one result may be corrupted per arm");
        assert!(matches!(first[0].values.get("a"), Some(Value::Null)));
    }

    #[test]
    fn a_non_matching_result_does_not_consume_the_arm() {
        let arms = arms_with(ArmFilter::TargetedOnly);
        let mut rows = [row(7)];
        assert!(!corrupt_with(&arms, &mut rows, Some(&AccessPath::FullScan)));
        assert!(!corrupt_with(&arms, &mut rows, None));
        assert!(!arms.lock()[0].fired);
        assert!(corrupt_with(
            &arms,
            &mut rows,
            Some(&AccessPath::PartitionLookup)
        ));
    }
}
