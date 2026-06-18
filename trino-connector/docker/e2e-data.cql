CREATE KEYSPACE IF NOT EXISTS analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS analytics.events (
  id int PRIMARY KEY,
  name text,
  score int,
  active boolean
);

INSERT INTO analytics.events (id, name, score, active) VALUES (1, 'alice', 10, true);
INSERT INTO analytics.events (id, name, score, active) VALUES (2, 'bob',   20, false);
INSERT INTO analytics.events (id, name, score, active) VALUES (3, 'carol', 30, true);
INSERT INTO analytics.events (id, name, score, active) VALUES (4, 'dave',  40, false);
INSERT INTO analytics.events (id, name, score, active) VALUES (5, 'erin',  50, true);
