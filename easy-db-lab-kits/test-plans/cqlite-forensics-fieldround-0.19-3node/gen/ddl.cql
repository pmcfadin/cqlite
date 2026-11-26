CREATE KEYSPACE IF NOT EXISTS fr WITH replication = {'class':'NetworkTopologyStrategy','dc1':3};
CREATE TYPE IF NOT EXISTS fr.addr (street text, city text, zip int);
CREATE TYPE IF NOT EXISTS fr.person (name text, born date, home frozen<addr>, tags frozen<set<text>>);
CREATE TABLE IF NOT EXISTS fr.t_basic (id bigint, bucket int, a ascii, t text, i int, b bigint, s smallint, ti tinyint,
  f float, d double, dec decimal, vi varint, bo boolean, ts timestamp, dt date, tm time, u uuid, tu timeuuid,
  ip inet, bl blob, du duration, PRIMARY KEY ((id), bucket));
CREATE TABLE IF NOT EXISTS fr.t_coll (id uuid PRIMARY KEY, l list<text>, s set<int>, m map<text,int>,
  fl frozen<list<int>>, fm frozen<map<text,frozen<set<int>>>>, nested map<text,frozen<list<text>>>)
  WITH compaction={'class':'LeveledCompactionStrategy'};
CREATE TABLE IF NOT EXISTS fr.t_udt (id int, ck int, p frozen<person>, ps list<frozen<person>>, tp frozen<tuple<int,text,uuid>>,
  PRIMARY KEY (id, ck));
CREATE TABLE IF NOT EXISTS fr.t_wide (pk int, ck timestamp, v text, n int, PRIMARY KEY (pk, ck))
  WITH CLUSTERING ORDER BY (ck DESC) AND compaction={'class':'TimeWindowCompactionStrategy',
  'compaction_window_unit':'HOURS','compaction_window_size':1};
CREATE TABLE IF NOT EXISTS fr.t_static (pk text, c1 int, c2 int, c3 text, s1 text STATIC, s2 map<text,int> STATIC, v blob,
  PRIMARY KEY ((pk, c1), c2, c3))
  WITH compaction={'class':'SizeTieredCompactionStrategy','enabled':'false'};
CREATE TABLE IF NOT EXISTS fr.t_tomb (id int, c1 int, c2 int, v text, l list<int>, m map<int,text>, PRIMARY KEY ((id), c1, c2))
  WITH gc_grace_seconds = 864000 AND compaction={'class':'SizeTieredCompactionStrategy','enabled':'false'};
CREATE TABLE IF NOT EXISTS fr.t_resurrect (id int, ck int, v text, PRIMARY KEY (id, ck)) WITH gc_grace_seconds = 864000
  AND compaction={'class':'SizeTieredCompactionStrategy','enabled':'false'};
CREATE TABLE IF NOT EXISTS fr.t_resurrect_purged (id int, ck int, v text, PRIMARY KEY (id, ck)) WITH gc_grace_seconds = 60
  AND compaction={'class':'SizeTieredCompactionStrategy','min_threshold':'2'};
