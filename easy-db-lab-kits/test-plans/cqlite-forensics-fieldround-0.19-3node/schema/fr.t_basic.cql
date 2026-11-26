CREATE KEYSPACE fr WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'}  AND durable_writes = true;

CREATE TYPE fr.addr (
    street text,
    city text,
    zip int
);

CREATE TYPE fr.person (
    name text,
    born date,
    home frozen<addr>,
    tags frozen<set<text>>
);

CREATE TABLE fr.t_basic (
    id bigint,
    bucket int,
    a ascii,
    b bigint,
    bl blob,
    bo boolean,
    d double,
    dec decimal,
    dt date,
    du duration,
    f float,
    i int,
    ip inet,
    s smallint,
    t text,
    ti tinyint,
    tm time,
    ts timestamp,
    tu timeuuid,
    u uuid,
    vi varint,
    PRIMARY KEY (id, bucket)
) WITH CLUSTERING ORDER BY (bucket ASC)
    AND additional_write_policy = '99p'
    AND allow_auto_snapshot = true
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND memtable = 'default'
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND incremental_backups = true
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
