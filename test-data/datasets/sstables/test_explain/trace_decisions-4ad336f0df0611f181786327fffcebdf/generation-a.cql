INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (1,1,'older') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (2,1,'row-shadowed') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (3,1,'range-shadowed') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (3,3,'outside-range') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (4,1,'equal-timestamp') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (5,1,'partition-shadowed') USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,m) VALUES (6,1,{'old':1}) USING TIMESTAMP 1000;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (7,1,'expires') USING TIMESTAMP 1000 AND TTL 3600;
INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (8,1,'purge-boundary') USING TIMESTAMP 1000;
