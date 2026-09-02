# Tasks — madvise-willneed-dontneed (issue #2824)

Outcome: **priced re-scope**, lead ruling on REQ-2824-03. The lever does not ship.

## 1. Build the lever (done, then reverted)
- [x] 1.1 Flip `mmap_advice_for` so `Auto` returns `Some(Advice::WillNeed)`.
- [x] 1.2 Retarget the #1143 pin to `Auto != Sequential`; correct every doc that said `Auto -> None`.
- [x] 1.3 `--lite` green; rust-reviewer, 5 findings, all fixed.
- [x] 1.4 **Reverted in full** after the blocker below. `Auto == None` is strictly stronger than
      `Auto != Sequential`, so the revert costs no protection.

## 2. Measure it
- [x] 2.1 Build both A/B arms from ONE tree differing by one match arm; verify by `strace` that they
      differ by exactly one `madvise` call. (An earlier pair built across a rebase was discarded.)
- [x] 2.2 Cold/warm A/B, page cache dropped per cold phase, arms alternated per round.
- [x] 2.3 Result: no effect, no regression. Scan-attributable major faults 4 vs 4; the device is EBS at
      132 MB/s with a 128 KiB window, so there is no headroom to detect the effect either way.
- [x] 2.4 Add a `--setup-only` FLOOR phase after roborev job 342 showed `%F` was not isolated to the
      scan mapping. It removed a spurious directional signal: raw 51 vs 49 became 4 vs 4.

## 3. The blocker
- [x] 3.1 roborev job 340: High — advising at open advises before the workload is known.
- [x] 3.2 Verify the load-bearing claim independently: `SSTableManager::new` opens EVERY SSTable under
      the data dir at `Database::open`. **Confirmed.**
- [x] 3.3 Establish that the fix needs the same scan-lifetime seam as AC2, so #2824's "policy flip on
      built machinery" premise fails for both halves.
- [x] 3.4 Escalate as REQ-2824-03 rather than decide it. Lead ruled option 1.

## 4. Land the re-scope
- [x] 4.1 Revert all `*.rs` and the two guide chapters to `origin/main`, byte for byte.
- [x] 4.2 Rewrite proposal + spec delta to the outcome; keep the durable requirements.
- [x] 4.3 Correct the two documents whose addenda assumed the flip shipped.
- [x] 4.4 Filed as **#3853** (unmilestoned), cross-referencing job 340 and the #2824 thread.
- [x] 4.5 `classify-docs-only.sh` -> **FULL PATH** (the harness `.sh` is a code input), so this diff
      is NOT code-free and roborev **does** apply. The lead's 'primary-source verification, not
      roborev' branch was conditional on the classifier; it did not fire.
- [x] 4.6 gate of record PASS (37/37); premerge-assert OK; merged via --auto 2026-09-01T22:17:19Z.
- [x] 4.7 Finalize: OpenSpec archived, telemetry stamped, closing comment posted.
