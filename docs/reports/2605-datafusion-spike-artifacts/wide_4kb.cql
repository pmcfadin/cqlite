-- Column/key structure verbatim from Apache Cassandra 5.0.2's own schema.cql for the
-- #2605 spike corpus. Storage options (compaction/compression/caching/...) are trimmed:
-- cqlite-core's parse_create_table does not accept the full option list, and none of
-- those options affect the DECODE contract this DDL supplies.
CREATE TABLE perf_2605.wide_4kb (
    pk text,
    ck int,
    body text,
    name text,
    note text,
    payload blob,
    status text,
    ts timestamp,
    v_bool boolean,
    v_dbl double,
    v_int int,
    v_long bigint,
    PRIMARY KEY (pk, ck)
) WITH CLUSTERING ORDER BY (ck ASC);
