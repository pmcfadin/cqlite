//! TEST-ONLY decoded-cell corruption seam (issue #4256).
//!
//! # Why this exists
//!
//! The differential/parity suites each prove their COMPARATOR can fail on a
//! synthetic diff, but none proved the WHOLE suite goes red when the real read
//! path hands back one wrong decoded value. This seam lets a meta-test corrupt
//! exactly one decoded cell of a real `SELECT` result — at the single chokepoint
//! where the executor's final rows are assembled — and then assert the suite's own
//! assertion fires. A suite that stays green under that corruption is comparing
//! something other than the path's output.
//!
//! # Scope
//!
//! * Compiled ONLY with the `result-corruption-injection` feature (NOT in defaults;
//!   no dependencies). Without it the module, the arming statics and the one call
//!   site in `select_executor::execute` do not exist, so every default build, the
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
}

static ARMS: Mutex<Vec<Arm>> = Mutex::new(Vec::new());
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static CORRUPTED: AtomicU64 = AtomicU64::new(0);

/// RAII arm: corrupts matching results until dropped.
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
    ARMS.lock().push(Arm { id, filter });
    CorruptionArm { id }
}

/// Number of results corrupted so far in this process: a meta-test reads it to prove
/// the arm actually FIRED (an arm that never fires would make a green suite look
/// like a failed meta-test for the wrong reason, or the reverse).
pub fn corrupted_count() -> u64 {
    CORRUPTED.load(Ordering::Relaxed)
}

/// The executor's call site: corrupt one cell of `rows` if an arm matches `path`.
pub(crate) fn maybe_corrupt(rows: &mut [QueryRow], path: Option<&AccessPath>) {
    let armed = ARMS.lock().iter().any(|a| a.filter.matches(path));
    if !armed {
        return;
    }
    let Some(row) = rows.first_mut() else {
        return;
    };
    let Some(name) = row.values.keys().min().cloned() else {
        return;
    };
    if let Some(value) = row.values.get_mut(&name) {
        *value = if matches!(value, Value::Null) {
            Value::Integer(0xBAD)
        } else {
            Value::Null
        };
        CORRUPTED.fetch_add(1, Ordering::Relaxed);
    }
}
