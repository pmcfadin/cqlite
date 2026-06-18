# Test Failure Investigation: cursor-compaction-completion vs trunk A/B

Date: 2026-06-10
Branch under test: `cursor-compaction-completion` (10 commits on top of trunk `3831d8265d`)
Goal: determine whether the failing suites from the full unit run (1,619 classes) are caused by the
cursor-compaction branch commits, or pre-exist on trunk.

## Summary

| Test | Branch result | Trunk (`3831d8265d`) result | Verdict |
|---|---|---|---|
| `service.accord.EpochSyncTest#test` | FAILS deterministically (pinned seed, step 7) | FAILS **identically** (same seed, same step 7, same stack) | Pre-existing **trunk bug**, introduced by CASSANDRA-21212 (`be4ddab9dd`); bisect-confirmed. Upstream-reportable (section below). |
| `security.CryptoProviderTest` (2 cases) | FAILS: `expected:<[AmazonCorrettoCryptoProvider]> but was:<[SUN]>` | FAILS **identically** (same 2 cases, same message) | Environmental: ACCP does not activate on this macOS/aarch64 + Corretto 21 JVM. Not branch-related. |
| `db.compaction.UnifiedCompactionStrategyTest#testDropExpiredSSTables` | Flaky: 1 fail in suite run + 1/3 solo runs | Flaky: **4/5** solo runs fail, same assertion (line 582) | Pre-existing flaky test; trunk is at least as flaky as the branch. Not branch-related. |
| `cql3.ViewComplexTTLTest` | Hang in parallel suite run only; passes solo (20/20) | n/a (not re-run; considered closed) | Parallel-runner artifact; out of scope here. |

**Conclusion: the trunk A/B confirms that none of the three failures is caused by the
cursor-compaction branch commits.** All three reproduce byte-for-byte (EpochSyncTest, CryptoProviderTest)
or at an equal-or-higher rate (UnifiedCompactionStrategyTest) on the exact trunk commit the branch
is based on, built from a clean checkout.

## Methodology

- Created a worktree at the exact branch-point commit:
  `git -C /Users/jhaddad/dev/cassandra worktree add /tmp/trunk-verify 3831d8265d`
  followed by `git submodule update --init --recursive` (accord submodule at `0a10cd0567`,
  identical to the main checkout's pointer).
- Built with `ant realclean && ant jar` (realclean is required for cross-branch builds in this repo:
  stale artifacts break on the `cql3.Terms` relocation and snakeyaml version changes). Build was clean
  (3,217 main sources compiled, accord built via its gradle wrapper).
- All test runs were sequential `ant testsome` invocations — never concurrent ant processes — to avoid
  the parallel-runner interference that produced the ViewComplexTTLTest hang.
- For the EpochSyncTest bisect, the same worktree was re-pointed (`git checkout --detach` +
  `git submodule update` + `ant realclean && ant jar`) at `be4ddab9dd^` and then `be4ddab9dd`.
- Environment: macOS 15.6.1 (24G90), Apple M4 Max (aarch64), OpenJDK Corretto 21.0.10.7.1
  (`21.0.10+7-LTS`).
- Branch-side evidence comes from the prior verification runs logged under `/tmp/cursor-soak/`
  (`verify-epoch.log`, `verify-crypto.log`, `verify-ucs-{1,2,3}.log`, `remaining-suite.log`).
- The worktree was removed after the investigation (`git worktree remove --force /tmp/trunk-verify`).

---

## 1. EpochSyncTest#test — pre-existing trunk regression (upstream-reportable)

### A/B evidence

Trunk worktree at `3831d8265d` (`/tmp/trunk-verify-epoch.log`) produced a failure **identical** to the
branch run (`/tmp/cursor-soak/verify-epoch.log`) — same seed, same failing step, same history, same stack:

```
Property error detected:
Seed = 152472217520379
Examples = 50
Pure = true
Error: java.lang.IllegalStateException
Steps: 500
Failing Step: 7
Values:
	State: Topology:
		1	Joined	-2926828829305835735
		: org.apache.cassandra.service.accord.EpochSyncTest.Cluster
	History:
		1: Bump Epoch 2
		2: Validate
		3: Bump Epoch 3
		4: Start Node 1; token=-2926828829305835735, epoch=4
		5: Validate
		6: Process Some
		7: 1 Start Joining; epoch=5

Caused by: java.lang.IllegalStateException
	at accord.utils.Invariants.require(Invariants.java:236)
	at accord.topology.ActiveEpochs.withNewEpochs(ActiveEpochs.java:84)
	at accord.topology.TopologyManager.updateActive(TopologyManager.java:433)
	at accord.topology.TopologyManager.reportTopology(TopologyManager.java:305)
	at org.apache.cassandra.service.accord.EpochSyncTest$Cluster.notify(EpochSyncTest.java:626)
	at org.apache.cassandra.service.accord.EpochSyncTest$Cluster.increment(EpochSyncTest.java:591)
```

The failure is deterministic because the test pins its seed in source
(`stateful().withSeed(152472217520379L).withExamples(50).withSteps(500)`,
`test/unit/org/apache/cassandra/service/accord/EpochSyncTest.java:143`); the seed was pinned by
`aa5c3aba1d` ("Improve Topology Management", 2025-10-28) and was presumably passing then.

### Root cause

The thrown invariant is `Invariants.require(epochs.length == 1)` in
`accord.topology.ActiveEpochs.withNewEpochs` (`modules/accord/accord-core/.../topology/ActiveEpochs.java:84`):

```java
ActiveEpochs withNewEpochs(ActiveEpoch[] epochs)
{
    long firstNonEmptyEpoch = this.firstNonEmptyEpoch;
    if (firstNonEmptyEpoch == -1 && epochs.length > 0 && !epochs[0].all().isEmpty())
    {
        Invariants.require(epochs.length == 1);   // <-- throws
        firstNonEmptyEpoch = epochs[0].epoch();
    }
    return new ActiveEpochs(manager, epochs, firstNonEmptyEpoch);
}
```

The scenario the seed produces: the CMS epoch is bumped several times while the Accord topology is
still empty (epochs 1–4: no joined nodes; node 1 *registers* at epoch 4, which adds no placements),
and only at epoch 5 (`PrepareJoin` / "Start Joining") does the first non-empty Accord topology appear.
At that point `TopologyManager` is already tracking more than one active epoch, so the new
`epochs.length == 1` requirement fails. In other words, the invariant assumes the first non-empty
epoch is always the *only* tracked epoch, which is false when multiple empty epochs precede the first
epoch that owns ranges (a plausible bootstrap sequence).

The previous accord version had no such requirement — the equivalent logic lived in the `ActiveEpochs`
constructor and simply set `firstNonEmptyEpoch = currentEpoch` whenever `epochs[0]` became non-empty,
regardless of array length.

### Bisect confirmation (all on the trunk-verify worktree, clean realclean+jar builds)

| Commit | accord submodule | EpochSyncTest result |
|---|---|---|
| `be4ddab9dd^` (= `57ecd4c101`) | `b81408ccbd` | **PASSES** (`Tests run: 1, Failures: 0`, 6.6 s) |
| `be4ddab9dd` (CASSANDRA-21212) | `0a10cd0567` | **FAILS** (step 7, `ActiveEpochs.java:84`) |
| `3831d8265d` (trunk tip / branch point) | `0a10cd0567` | FAILS identically |
| branch `cursor-compaction-completion` | `0a10cd0567` | FAILS identically |

The breaking change is therefore the accord submodule bump in
**`be4ddab9dd` — " Safely regain ranges and delete retired command stores" (CASSANDRA-21212,
patch by Alan Wang; reviewed by Benedict, 2026-06-04)**, which pulls accord commit
`0a10cd056794c05588114f45ce86d49d6d6538db` ("Safely regain ranges and delete retired command stores",
2026-06-04). `git log -S 'require(epochs.length == 1'` in `modules/accord` shows that accord commit is
the one that introduced the invariant (the `withNewEpochs` method itself is new in that commit).

Note: CASSANDRA-21212 already has known test fallout — trunk carries
`24717dbb0c` ("ninja follow-up to CASSANDRA-21212: fix AccordVirtualTablesTest.tableUpdates").

### Suggested upstream report (self-contained)

- **Title suggestion:** `EpochSyncTest fails deterministically since CASSANDRA-21212: ActiveEpochs.withNewEpochs requires epochs.length == 1 when first non-empty epoch follows multiple empty epochs`
- **JIRA reference:** CASSANDRA-21212 (introducing commit `be4ddab9dd`, accord `0a10cd0567`).
- **Repro:** `ant testsome -Dtest.name=org.apache.cassandra.service.accord.EpochSyncTest`
  on trunk at/after `be4ddab9dd`. Deterministic — the seed `152472217520379` is pinned in the test
  source (EpochSyncTest.java:143), 50 examples / 500 steps; fails at example 1, step 7
  ("1 Start Joining; epoch=5") in < 1 s.
- **Failure:** `IllegalStateException` from `Invariants.require(epochs.length == 1)` at
  `accord.topology.ActiveEpochs.withNewEpochs(ActiveEpochs.java:84)` via
  `TopologyManager.reportTopology` (full output quoted above).
- **Trigger:** several empty CMS/Accord epochs (only epoch bumps and a node registration) followed by
  the first epoch with non-empty placements (node begins joining); the first non-empty epoch arrives
  while more than one epoch is actively tracked.
- **Last good / first bad:** passes at `57ecd4c101` (parent of `be4ddab9dd`), fails at `be4ddab9dd`;
  verified with clean `ant realclean` builds and `git submodule update` at each commit.
- **Environment:** macOS 15.6.1 (Apple M4 Max, aarch64), OpenJDK Corretto 21.0.10 (21.0.10+7-LTS),
  `ant testsome`. Nothing environment-specific is suspected — the failure is a deterministic
  seeded property failure.

---

## 2. CryptoProviderTest — environmental, identical on trunk

Branch (`/tmp/cursor-soak/verify-crypto.log`):

```
Testsuite: org.apache.cassandra.security.CryptoProviderTest-_jdk21 Tests run: 17, Failures: 2, Errors: 0, Skipped: 0, Time elapsed: 1.101 sec
Testcase: testCryptoProviderInstallation(...)-_jdk21:	FAILED
Testcase: testProviderInstallsJustOnce(...)-_jdk21:	FAILED
expected:<[AmazonCorrettoCryptoProvider]> but was:<[SUN]>
```

Trunk worktree (`/tmp/trunk-verify-crypto.log`):

```
Testsuite: org.apache.cassandra.security.CryptoProviderTest-_jdk21 Tests run: 17, Failures: 2, Errors: 0, Skipped: 0, Time elapsed: 1.106 sec
Testcase: testCryptoProviderInstallation(...)-_jdk21:	FAILED
Testcase: testProviderInstallsJustOnce(...)-_jdk21:	FAILED
expected:<[AmazonCorrettoCryptoProvider]> but was:<[SUN]>
```

Same 2 of 17 cases, same assertion text, on both sides. **Classification: environmental.** The test
expects the Amazon Corretto Crypto Provider (ACCP) to install as the top JCE provider; on this
macOS/aarch64 + Corretto 21 JVM, ACCP does not activate and the default `SUN` provider remains first.
No branch commit touches security/crypto code. No action needed for the branch; not an upstream bug
(known platform limitation of ACCP availability).

## 3. UnifiedCompactionStrategyTest#testDropExpiredSSTables — pre-existing flake, worse on trunk

Failing assertion (both sides, always the same):
`assertNotNull(pick)` at `UnifiedCompactionStrategyTest.java:582` in `testDropExpiredFromBucket`
(called from `testDropExpiredSSTables`, line 545) — `strategy.getNextCompactionPick(expirationPoint)`
returned null even though every sstable is past its max local deletion time. The test wires a real RNG
into the mocked controller (`when(controller.random()).thenCallRealMethod()`, line 564), so pick
selection is genuinely randomized per run.

| Side | Runs | testDropExpiredSSTables failures | Rate |
|---|---|---|---|
| Branch (suite run, `remaining-suite.log`) | 1 | 1 (`Tests run: 27, Failures: 1`) | — |
| Branch solo (`verify-ucs-{1,2,3}.log`) | 3 | 1 (run 1: line 582; runs 2–3 clean 27/27) | ~1/3 |
| Trunk solo (`/tmp/trunk-verify-ucs-{1..5}.log`) | 5 | 4 (runs 1,2,4,5: line 582; run 3 clean 27/27) | 4/5 |

Trunk reproduces the identical failure at an equal-or-higher rate than the branch (4/5 vs ~1/3–2/4).
The branch only touches the cursor compaction write path
(`9bc339b6cf..a0c23c5f4d`: differential harness, allocation gates, stats fix, sparse-row decoding) —
nothing in UCS pick selection. **Classification: pre-existing flaky test on trunk, RNG-dependent;
not caused by, and not made worse by, the branch.** (Recent trunk history around the test:
`b17e4ee88a` "Fix SSTableReader interval mock in UnifiedCompactionStrategyTest", `763bcf2de5`
"Fast single-partition Ephemeral Reads", `3dc33de6ea` import reorg — candidates if anyone wants to
chase the flake upstream, but that is outside this branch's scope.)

## 4. ViewComplexTTLTest — closed previously

Hung only during the parallel full-suite run; passes solo on the branch (20/20,
`BUILD SUCCESSFUL` in `/tmp/cursor-soak/verify-view.log`, and the suite log shows
`Tests run: 20, Failures: 0, Errors: 0` when it completed). Treated as a parallel-runner artifact;
no trunk A/B performed and none needed.

---

## Final conclusion

The A/B comparison against the exact branch-point trunk commit `3831d8265d` (clean realclean builds,
sequential runs, identical accord submodule) confirms that **none of the three investigated failures
is caused by the 10 cursor-compaction-completion commits**:

1. **EpochSyncTest** — deterministic trunk regression from CASSANDRA-21212 (`be4ddab9dd`,
   accord `0a10cd0567`); bisected to that exact commit (parent passes, commit fails). Should be
   reported upstream (template above).
2. **CryptoProviderTest** — identical environmental failure (ACCP unavailable on macOS/aarch64
   Corretto 21) on both branch and trunk.
3. **UnifiedCompactionStrategyTest#testDropExpiredSSTables** — same RNG-dependent flake on both sides;
   trunk failed 4/5 solo runs vs the branch's ~1 in 3–4.

Run artifacts: `/tmp/trunk-verify-build.log`, `/tmp/trunk-verify-epoch.log`,
`/tmp/trunk-verify-crypto.log`, `/tmp/trunk-verify-ucs-{1..5}.log`, `/tmp/trunk-verify-prev.log`
(parent commit), `/tmp/trunk-verify-at21212.log` (CASSANDRA-21212 commit); branch artifacts under
`/tmp/cursor-soak/`. The `/tmp/trunk-verify` worktree has been removed.
