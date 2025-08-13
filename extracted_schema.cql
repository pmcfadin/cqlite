-- Schema extracted from Cassandra 5 Docker container
-- This is the schema that was loaded and created the SSTable files

CREATE KEYSPACE cqlite_test WITH REPLICATION = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
};

USE cqlite_test;

-- Basic table with common CQL types
CREATE TABLE demo_table (
  id UUID PRIMARY KEY,
  name TEXT,
  age INT,
  score DOUBLE,
  active BOOLEAN,
  created_at TIMESTAMP
);

-- Table with collections (Lists, Sets, Maps)
CREATE TABLE collections_demo (
  id UUID PRIMARY KEY,
  tags SET<TEXT>,
  attributes MAP<TEXT, INT>,
  scores LIST<DOUBLE>
);

-- Sample data that was inserted:
-- INSERT INTO demo_table (id, name, age, score, active, created_at) 
-- VALUES (uuid(), 'Alice Johnson', 30, 95.5, true, '2024-01-15 10:30:00');
-- INSERT INTO demo_table (id, name, age, score, active, created_at)
-- VALUES (uuid(), 'Bob Smith', 25, 87.2, true, '2024-01-16 14:22:00');
-- INSERT INTO demo_table (id, name, age, score, active, created_at)
-- VALUES (uuid(), 'Carol Davis', 35, 92.8, false, '2024-01-17 09:15:00');

-- INSERT INTO collections_demo (id, tags, attributes, scores)
-- VALUES (uuid(), {'database', 'cassandra', 'nosql'}, {'priority': 1, 'level': 5}, [88.5, 92.1, 94.7]);
-- INSERT INTO collections_demo (id, tags, attributes, scores)
-- VALUES (uuid(), {'testing', 'development'}, {'priority': 2, 'level': 3}, [78.3, 85.9]);