INSERT INTO risk_cases (id,ck,v) VALUES (1,1,'newer-than-row-tombstone') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (3,1,'tie-with-row-tombstone') USING TIMESTAMP 2000;
INSERT INTO risk_cases (id,ck,v) VALUES (4,1,'b-newer') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (5,1,'newer-than-cell-tombstone') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (6,1,'b-older') USING TIMESTAMP 3000;
INSERT INTO risk_cases (id,ck,v) VALUES (7,1,'only-in-b') USING TIMESTAMP 3000;
