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

-- Diverse-type table to exercise uuid + timestamp conversion end-to-end.
CREATE TABLE IF NOT EXISTS analytics.typed (
  id uuid PRIMARY KEY,
  label text,
  amount int,
  created timestamp,
  active boolean
);

INSERT INTO analytics.typed (id, label, amount, created, active)
  VALUES (11111111-1111-1111-1111-111111111111, 'alpha', 100, '2024-01-01 00:00:00+0000', true);
INSERT INTO analytics.typed (id, label, amount, created, active)
  VALUES (22222222-2222-2222-2222-222222222222, 'beta',  200, '2024-06-15 12:30:00+0000', false);
