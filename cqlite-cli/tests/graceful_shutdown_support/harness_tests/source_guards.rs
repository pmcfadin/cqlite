//! **SOURCE-LEVEL INVARIANTS OF THE HARNESS ITSELF** (issue #3652).
//!
//! Split out of `harness_tests.rs` under the campsite rule (#1135), by SUBJECT:
//! everything here is a property of EVERY site rather than of one execution, so it
//! is answered from the harness's own text. That is deliberately a different KIND
//! of test from its siblings, and it exists for a failure the others structurally
//! cannot catch — `kill_and_reap` was added and then applied at only some of its
//! call sites, so the harness held a helper AND the defect it was written to remove,
//! with a behavioural test passing over the one path it did cover.
//!
//! A source scan buys reach and pays in precision; each test below states its own
//! limits where a reader will see them.
//!
//! Deliberately NO `use super::*`: a scan of text needs none of the harness's types,
//! and importing them unused is a warning under the gate's `-D warnings`.

/// **NO FAILURE PATH KILLS A CHILD WITHOUT REAPING IT** — asserted over the
/// harness's own SOURCE, because the claim is about every site and no behavioural
/// test can see a site nobody wrote yet (#3652, roborev job 265 finding 4).
///
/// WHY THIS SHAPE. `kill_and_reap` was added and then applied at only some of its
/// call sites: four failure paths in `graceful_shutdown_tests.rs` — the ones that
/// run against a REAL child — kept the bare `let _ = child.kill();`, which signals
/// and never waits, leaving the zombie the helper exists to eliminate. The
/// behavioural test above (`a_missing_acknowledgement_kills_and_reaps_the_child…`)
/// pins ONE path and cannot speak for the others, and the next failure path someone
/// adds is exactly where this regresses. So the property "every teardown goes
/// through the one helper" is checked where it lives: in the text.
///
/// The sources are read with `include_str!`, i.e. at COMPILE time and relative to
/// THIS file, so the check cannot silently pass because a runtime path or the
/// working directory moved — a scan that finds no files is the classic vacuous
/// green, and here an unreadable path is a build error instead.
///
/// **WHAT IT DOES NOT COVER, stated because a guard that overstates its reach is
/// worse than none.** It is a TEXT scan, not a parse: a `.kill()` inside a block
/// comment counts as code, one written through an alias or a different receiver
/// spelling is not seen, and `libc::kill(pid, …)` — which stage (c) uses to deliver
/// SIGINT, correctly — is deliberately not matched. It reads only the files listed
/// below; a new file in this harness joins the scan only when someone adds it here.
#[test]
fn no_failure_path_kills_a_child_without_reaping_it() {
    // Split so this scanner cannot match its own needle, the way the repo's other
    // source guards are written.
    let needle = concat!(".kil", "l()");
    let allow = concat!("kill-and-reap", "-allow");
    let sources: &[(&str, &str)] = &[
        (
            "graceful_shutdown_tests.rs",
            include_str!("../../graceful_shutdown_tests.rs"),
        ),
        (
            "graceful_shutdown_support/mod.rs",
            include_str!("../mod.rs"),
        ),
        (
            "graceful_shutdown_support/transcript.rs",
            include_str!("../transcript.rs"),
        ),
        (
            "graceful_shutdown_support/budgets.rs",
            include_str!("../budgets.rs"),
        ),
        (
            "graceful_shutdown_support/harness_tests.rs",
            include_str!("../harness_tests.rs"),
        ),
        (
            "graceful_shutdown_support/harness_tests/awaited_stream_tests.rs",
            include_str!("awaited_stream_tests.rs"),
        ),
        (
            "graceful_shutdown_support/budgets/census_tests.rs",
            include_str!("../budgets/census_tests.rs"),
        ),
        (
            "graceful_shutdown_support/budgets/calibration_tests.rs",
            include_str!("../budgets/calibration_tests.rs"),
        ),
    ];

    let mut offenders = Vec::new();
    let mut allowed = 0usize;
    let mut scanned = 0usize;
    for (name, text) in sources {
        scanned += 1;
        // The allow marker may sit on the call's own line or in the comment block
        // immediately above it, so a rationale does not have to fit on one line.
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if !line.contains(needle) {
                continue;
            }
            let trimmed = line.trim_start();
            // A line-comment mention is prose, not a teardown — this file and
            // `mod.rs` both DISCUSS the bare kill in comments on purpose.
            if trimmed.starts_with("//") {
                continue;
            }
            let context_marked = lines[i.saturating_sub(6)..=i]
                .iter()
                .any(|l| l.contains(allow));
            if context_marked {
                allowed += 1;
                continue;
            }
            offenders.push(format!("{name}:{}: {}", i + 1, trimmed));
        }
    }

    assert_eq!(
        scanned, 8,
        "the scan must cover every file of this harness, or it reports about a subset"
    );
    assert_eq!(
        allowed, 1,
        "exactly ONE `.kill()` may carry the allow marker — the signal inside \
         `kill_and_reap`, which reaps on the next line. Found {allowed}: either a teardown was \
         marked instead of being routed through the helper, or the helper's own call lost its \
         marker (which would make this guard pass vacuously)"
    );
    assert!(
        offenders.is_empty(),
        "these sites kill a child WITHOUT reaping it, so each leaves the zombie — and the \
         reader/collector threads blocked on its pipes — that `kill_and_reap` exists to \
         eliminate (#3652, roborev job 265 finding 4):\n  {}\n\
         Route each through `kill_and_reap(&mut child)` and fold its returned sentence into the \
         panic message: what happened to the child is part of the evidence a failure reports. If \
         a site genuinely must signal without reaping, mark it `{allow}` with a rationale.",
        offenders.join("\n  ")
    );
}
