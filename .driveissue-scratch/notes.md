Resumed after box restart (Cloud Custodian stop/restart, /tmp wiped).
Adopted stale marker (prior session pid confirmed dead) and reclaimed lane lock.
Full gate PASSED historically at 0074b549f but its artifact was lost with /tmp wipe.
Spawned flow-closer-4309 (opus) to: resolve live roborev job 42, get one final
sanctioned roborev round posted to PR #4312, run ONE fresh full gate of record
(artifacts under /data/gate-artifacts/issue-4309, not /tmp), request spec-auditor
(C) from lead via NEEDS-SPAWN, then premerge-assert + merge --auto + finalize.
Follow-ups already filed: #4311 (gate-wiring), #4314 (batched roborev Lows).
Awaiting flow-closer-4309's first report (expected: NEEDS-SPAWN: spec-auditor,
or a blocker).
