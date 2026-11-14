INSERT INTO test_explain.trace_decisions (id,ck,v) VALUES (1,1,'newer') USING TIMESTAMP 2000;
DELETE FROM test_explain.trace_decisions USING TIMESTAMP 2000 WHERE id=2 AND ck=1;
DELETE FROM test_explain.trace_decisions USING TIMESTAMP 2000 WHERE id=3 AND ck>=1 AND ck<=2;
DELETE v FROM test_explain.trace_decisions USING TIMESTAMP 1000 WHERE id=4 AND ck=1;
DELETE FROM test_explain.trace_decisions USING TIMESTAMP 2000 WHERE id=5;
UPDATE test_explain.trace_decisions USING TIMESTAMP 2000 SET m={'new':2} WHERE id=6 AND ck=1;
DELETE v FROM test_explain.trace_decisions USING TIMESTAMP 2000 WHERE id=8 AND ck=1;
