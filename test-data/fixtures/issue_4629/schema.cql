CREATE KEYSPACE fix4629 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1};
CREATE TABLE fix4629.t (id int, c int, v text, l list<int>, m map<int,text>, PRIMARY KEY (id, c)) WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'} AND compression = {'enabled': 'false'};
