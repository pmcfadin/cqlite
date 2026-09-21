CREATE KEYSPACE test_explain WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE test_explain.trace_decisions (
    id int,
    ck int,
    v text,
    m map<text, int>,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': false}
  AND gc_grace_seconds = 864000;
