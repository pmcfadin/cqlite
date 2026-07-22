CREATE KEYSPACE IF NOT EXISTS analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS analytics.events (
  id int PRIMARY KEY,
  name text,
  score int,
  ratio double,
  active boolean
);

INSERT INTO analytics.events (id, name, score, ratio, active) VALUES (1, 'alice', 10, 1.5, true);
INSERT INTO analytics.events (id, name, score, ratio, active) VALUES (2, 'bob',   20, 2.5, false);
INSERT INTO analytics.events (id, name, score, ratio, active) VALUES (3, 'carol', 30, 3.5, true);
INSERT INTO analytics.events (id, name, score, ratio, active) VALUES (4, 'dave',  40, 4.5, false);
INSERT INTO analytics.events (id, name, score, ratio, active) VALUES (5, 'erin',  50, 5.5, true);

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

-- Wide table for the AUTOMATIC aggregation-pushdown cardinality gate (issue #944).
-- 2 partitions (device) × 5 clustering rows (ts) = 10 rows. The authoritative
-- per-table stats surfaced over the Flight table_stats action are:
--   partition_count = 2, total_rows = 10.
-- The DDL-driven gate then estimates:
--   GROUP BY device           -> groups ≈ 2/10 = 0.2  (< 0.5) -> PUSH (low cardinality)
--   GROUP BY device, ts       -> full row uniqueness ≈ 1.0    (> 0.5) -> DECLINE (high)
-- so a high-cardinality GROUP BY is left to Trino while a low-cardinality one and
-- globals still push. Results are identical either way (pushdown is an optimization);
-- the e2e asserts the GATE via EXPLAIN, and the answers via the query results.
CREATE TABLE IF NOT EXISTS analytics.readings (
  device int,
  ts int,
  value int,
  PRIMARY KEY (device, ts)
);

INSERT INTO analytics.readings (device, ts, value) VALUES (1, 1, 10);
INSERT INTO analytics.readings (device, ts, value) VALUES (1, 2, 20);
INSERT INTO analytics.readings (device, ts, value) VALUES (1, 3, 30);
INSERT INTO analytics.readings (device, ts, value) VALUES (1, 4, 40);
INSERT INTO analytics.readings (device, ts, value) VALUES (1, 5, 50);
INSERT INTO analytics.readings (device, ts, value) VALUES (2, 1, 11);
INSERT INTO analytics.readings (device, ts, value) VALUES (2, 2, 22);
INSERT INTO analytics.readings (device, ts, value) VALUES (2, 3, 33);
INSERT INTO analytics.readings (device, ts, value) VALUES (2, 4, 44);
INSERT INTO analytics.readings (device, ts, value) VALUES (2, 5, 55);

-- Typed collection columns end-to-end (issue #2815). A list<frozen<udt>> column
-- projects through the connector as Trino array(varchar) whose elements are the
-- server-decoded UDT strings; a plain list<text> and a map<text,int> exercise the
-- other collection shapes.
--
-- Empty-vs-absent semantics follow Cassandra 5.0: a NON-FROZEN empty collection is
-- NOT stored (it writes no cells) and is therefore indistinguishable from absent —
-- SELECT returns null in BOTH cases. Only a FROZEN collection is stored as a single
-- self-describing value, so an empty FROZEN collection IS persisted and reads back as
-- a non-null empty array. `notes frozen<list<text>>` carries that genuine
-- empty-vs-null distinction; addrs/tags/attrs (all non-frozen) read null when empty.
CREATE TYPE IF NOT EXISTS analytics.simpleaddr (street text, zip int);

CREATE TABLE IF NOT EXISTS analytics.contacts (
  id int PRIMARY KEY,
  name text,
  addrs list<frozen<simpleaddr>>,
  tags list<text>,
  attrs map<text, int>,
  notes frozen<list<text>>
);

-- Multi-element row: two UDT addresses, two tags, two map entries, one frozen note.
INSERT INTO analytics.contacts (id, name, addrs, tags, attrs, notes) VALUES (
  1, 'alice',
  [{street: '12 Oak St', zip: 94040}, {street: '9 Elm Ave', zip: 94041}],
  ['home', 'work'],
  {'a': 1, 'b': 2},
  ['vip']
);
-- Empty-collection row: the NON-FROZEN empty list/map (addrs/tags/attrs) are NOT
-- stored → read back as null (Cassandra parity, NOT an empty array). The FROZEN
-- empty list `notes []` IS stored → reads back as a non-null empty array, so it is
-- the one column that genuinely distinguishes empty from absent.
INSERT INTO analytics.contacts (id, name, addrs, tags, attrs, notes) VALUES (
  2, 'bob', [], [], {}, []
);
-- Absent row: addrs/tags/attrs/notes never set (→ null).
INSERT INTO analytics.contacts (id, name) VALUES (3, 'carol');
