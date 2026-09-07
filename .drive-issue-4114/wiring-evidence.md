# Wiring evidence for #4114 — the fix works through a PUBLIC SURFACE
Verified by the lead directly (no subagent reported this), on the committed
Cassandra-5.0.8-written fixture, at commit 9f34215c5.

CLAUDE.md: "A feature is done only when its public surface exercises it." The unit tests
assert against byte literals; the integration test asserts against the sstabledump goldens;
THIS is the end-to-end user-visible proof.

## Command (reproducible)
    cargo build -p cqlite-cli
    ./target/debug/cqlite read-sstable \
      test-data/fixtures/issue_4114/test_vector/vector_exact-*/nb-1-big-Data.db --format json

## BEFORE (origin/main behaviour, measured 00:31Z — see ac1-measurement.md)
    exit 0, NO error, NO warning on stderr
    [ { "key": "RowKey([0, 0, 0, 1])", "value": "{v3: 0x0000003f80000040000000}" },
      { "key": "RowKey([0, 0, 0, 2])", "value": "{v3: 0x00000040900000c0a00000}" } ]
The 11-byte TAIL of the vector's own 12 bytes, presented as a hex blob. Silent wrong data.

## AFTER (this branch)
    exit 0, stderr carries no error and no warning
    [ { "key": "RowKey([0, 0, 0, 1])", "value": "{v3: [2.4651903e-32, 1, 2]}" },
      { "key": "RowKey([0, 0, 0, 2])", "value": "{v3: [2.4651903e-32, 4.5, -5]}" } ]

## Cross-check against the ORACLE (Cassandra's own sstabledump goldens)
    row 1  Cassandra: [2.4651903e-32, 1.0, 2.0]   CQLite: [2.4651903e-32, 1, 2]     MATCH
    row 2  Cassandra: [2.4651903e-32, 4.5, -5.0]  CQLite: [2.4651903e-32, 4.5, -5]  MATCH
(`1` vs `1.0` and `-5` vs `-5.0` are JSON number RENDERING, not value differences; the
integration test compares numerically against the golden, not textually.)
