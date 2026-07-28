--- DESCRIBE TABLE ws0.events
keyspace_name	type	name	create_statement
ws0	table	events	CREATE TABLE ws0.events (
    part_id text,
    seq int,
    event_time timestamp,
    blob_a blob,
    blob_b blob,
    device_id uuid,
    metric_a int,
    metric_b bigint,
    metric_c double,
    payload text,
    region text,
    status text,
    PRIMARY KEY (part_id, seq, event_time)
) WITH CLUSTERING ORDER BY (seq ASC, event_time ASC)
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
