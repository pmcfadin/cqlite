> SCOPE CHANGE (2026-06-11): fix lands on the cursor-compaction branch directly; NO 4.1/5.0 backports. Draft kept as technical reference.

# JIRA draft: SSTableRewriter deletes original sstables despite keepOriginals=true when early open is active

> Draft for a standalone JIRA. NOT committed; copy into the ticket.
> Versions verified affected: trunk, cassandra-5.0, cassandra-4.1 (same code in all three).

## Summary

`SSTableRewriter`'s `keepOriginals` flag promises that the rewrite will not obsolete the
original sstables — their files stay on disk after the transaction commits. That promise is
only honored when early open (`sstable_preemptive_open_interval`) is inactive or the
transaction is offline. On an ONLINE transaction with early open active, every original that
becomes fully covered by the new output is obsoleted anyway, and its files are deleted when
the last reader reference is released.

## Background

During an incremental rewrite with early open, `moveStarts(lowerbound)` migrates reads from
the originals to the partially-written output by moving each original reader's start key
forward. When an original's start moves past its last key — the new output fully covers it —
`moveStarts` eagerly obsoletes it:

```java
if (lowerbound.compareTo(latest.getLast()) >= 0)
{
    if (!transaction.isObsolete(latest))
        transaction.obsolete(latest);     // <-- no keepOriginals check
    continue;
}
```

The `keepOriginals` flag is consulted in exactly one place, the bulk obsoletion in
`doPrepare()`:

```java
if (!keepOriginals)
    transaction.obsoleteOriginals();
```

`moveStarts` also runs unconditionally on writer switch/finish (not just at the periodic
early-open boundary), so even small rewrites obsolete all fully-covered originals. Since a
full-range rewrite fully covers every input, `keepOriginals=true` + online transaction +
early open enabled deletes every original — just deferred to last-reference release, which
makes it easy to miss: the files are still present immediately after the rewrite returns and
disappear shortly afterwards.

`moveStarts` returns early when `transaction.isOffline() || preemptiveOpenInterval ==
Long.MAX_VALUE`. The shipping users of `keepOriginals` (scrub, sstableupgrade tooling) run
offline transactions, so the broken combination has no in-tree production caller today — this
is a latent contract violation rather than live data loss. Any future or external caller
relying on `keepOriginals` as an online backup mechanism would silently lose the backups.

## Reproduction

Found by a differential test harness that runs `CompactionTask(cfs, txn, gcBefore,
keepOriginals = true)` on an online transaction and re-opens the input descriptors afterwards:
with default early-open settings the inputs' Data components are gone (asynchronously, after
the output readers' references are released); with
`DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1)` the originals survive as
documented. A minimal regression test is included in the patch: flush several sstables,
compact with keepOriginals=true and early open enabled, force reference release, assert the
original files still exist.

## Proposed fix

Honor the flag in `moveStarts`:

```java
if (!keepOriginals && !transaction.isObsolete(latest))
    transaction.obsolete(latest);
```

With `keepOriginals`, a fully-covered original then simply stays in the view until commit,
where the existing `keepOriginals` path delists it without deleting its files (identical to
the behavior when early open is disabled). Reads in the covered range may consult both the
original and the new output for the remainder of the rewrite; reconciliation makes that
correct, the cost is a transient extra merge source — the same cost the
early-open-disabled keepOriginals path already pays for the whole rewrite.

Alternative considered: document/assert that `keepOriginals` requires an offline transaction.
Rejected as the weaker option — the flag's name and comment ("true if we do not want to
obsolete the originals") state a behavior, and the behavior is implementable in one line.

## Patch contents (per branch: 4.1, 5.0, trunk)

- `SSTableRewriter.moveStarts`: one-line guard change (the line is textually identical on all
  three branches; surrounding context differs slightly on 4.1 — key-cache invalidation — but
  does not affect the change).
- `SSTableRewriterTest`: new regression test (~40-80 lines; the file currently has no
  keepOriginals coverage at all).
- CHANGES.txt entry.

---

## Pickup notes for a fresh session (not part of the JIRA text)

- **Where the change goes:** `SSTableRewriter.moveStarts`, the fully-covered branch
  (`lowerbound.compareTo(latest.getLast()) >= 0`). Trunk: `transaction.obsolete(latest)` at
  ~line 232, guard at ~349. 5.0: guard at 343-344. 4.1: guard at 386-387; 4.1's moveStarts has
  extra key-cache invalidation code and uses `latest.first/.last` instead of getters, but the
  obsoletion line is identical. Verify with:
  `git show cassandra-5.0:src/java/org/apache/cassandra/io/sstable/SSTableRewriter.java`.
- **Branch logistics:** cassandra-4.1 and cassandra-5.0 are checked out as WORKTREES in this
  repo (see `git branch`). Apache merge flow is fix on the oldest affected branch first
  (4.1), then merge up 4.1 -> 5.0 -> trunk. After switching branches in any working tree,
  `ant realclean` before building — stale build artifacts from other branches caused bogus
  compile failures (cql3.Terms relocation, snakeyaml 2.1-vs-2.4) in this checkout before.
- **Regression test recipe** (new test in `test/unit/.../io/sstable/SSTableRewriterTest.java`,
  which has zero keepOriginals coverage today):
  1. Write + flush 2-3 sstables; record their `descriptor`s.
  2. Ensure early open is ACTIVE: `DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(1)`
     (moveStarts also runs unconditionally at writer finish whenever the interval !=
     Long.MAX_VALUE, so small data suffices — no need to write 1MiB).
  3. Online transaction: `cfs.getTracker().tryModify(sstables, OperationType.COMPACTION)`,
     then `new CompactionTask(cfs, txn, gcBefore, true /* keepOriginals */)
     .execute(ActiveCompactionsTracker.NOOP)` — or drive SSTableRewriter directly as other
     tests in that file do, passing keepOriginals=true.
  4. CRITICAL: file deletion is ASYNC — it happens when the last reader reference is
     released, on NonPeriodicTasks. After the rewrite, release output reader references (or
     `LifecycleTransaction.waitForDeletions()` — that helper exists for exactly this) before
     asserting. Without the fix the inputs' Data components are gone; with it they survive.
     "Files present right after compaction" proves nothing.
  5. Negative control: same test with keepOriginals=false must show the files deleted —
     guards against the test passing vacuously.
- **Full diagnostic trail** (byte-level evidence, how it was found): finding #1 in
  `garbage-free-compaction-improvements/cursor-compaction-plan.md` on branch `cursor-compaction-completion` (untracked file,
  reviewer-notes section near the end). The differential harness that found it:
  `test/unit/org/apache/cassandra/db/compaction/differential/DifferentialCompactionTester.java`
  (committed on that branch) — its workaround (disabling preemptive open) can be REMOVED once
  this fix lands on trunk, which also serves as an end-to-end confirmation.
