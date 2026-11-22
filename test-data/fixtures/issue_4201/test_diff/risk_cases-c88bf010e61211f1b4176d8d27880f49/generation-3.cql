INSERT INTO risk_cases (id,ck,v) VALUES (19,1,'reinserted') USING TIMESTAMP 7000;
DELETE v FROM risk_cases USING TIMESTAMP 6000 WHERE id=20 AND ck=1;
INSERT INTO risk_cases (id,ck,v) VALUES (22,1,'expiring') USING TIMESTAMP 9000 AND TTL 5;
