-- Wide Rows Schema for CQL Test Data Generation
-- Tests wide partitions with many clustering columns and large data
-- Issue #18: Docker-based test data generation

-- Create keyspace for wide rows testing
CREATE KEYSPACE IF NOT EXISTS test_wide_rows WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
} AND durable_writes = true;

USE test_wide_rows;

-- Wide partition with many clustering columns
CREATE TABLE IF NOT EXISTS wide_partition_table (
    partition_key UUID,
    clustering_col1 TIMESTAMP,
    clustering_col2 TEXT,
    clustering_col3 INT,
    clustering_col4 UUID,
    clustering_col5 DATE,
    data_column TEXT,
    value_column BIGINT,
    blob_column BLOB,
    json_column TEXT,
    PRIMARY KEY (partition_key, clustering_col1, clustering_col2, clustering_col3, clustering_col4, clustering_col5)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND clustering ORDER BY (clustering_col1 DESC, clustering_col2 ASC, clustering_col3 DESC, clustering_col4 ASC, clustering_col5 DESC);

-- Very wide table with many columns (hundreds of columns)
CREATE TABLE IF NOT EXISTS many_columns_table (
    id UUID PRIMARY KEY,
    col_001 TEXT, col_002 TEXT, col_003 TEXT, col_004 TEXT, col_005 TEXT,
    col_006 TEXT, col_007 TEXT, col_008 TEXT, col_009 TEXT, col_010 TEXT,
    col_011 INT, col_012 INT, col_013 INT, col_014 INT, col_015 INT,
    col_016 INT, col_017 INT, col_018 INT, col_019 INT, col_020 INT,
    col_021 BIGINT, col_022 BIGINT, col_023 BIGINT, col_024 BIGINT, col_025 BIGINT,
    col_026 FLOAT, col_027 FLOAT, col_028 FLOAT, col_029 FLOAT, col_030 FLOAT,
    col_031 DOUBLE, col_032 DOUBLE, col_033 DOUBLE, col_034 DOUBLE, col_035 DOUBLE,
    col_036 BOOLEAN, col_037 BOOLEAN, col_038 BOOLEAN, col_039 BOOLEAN, col_040 BOOLEAN,
    col_041 TIMESTAMP, col_042 TIMESTAMP, col_043 TIMESTAMP, col_044 TIMESTAMP, col_045 TIMESTAMP,
    col_046 UUID, col_047 UUID, col_048 UUID, col_049 UUID, col_050 UUID,
    col_051 BLOB, col_052 BLOB, col_053 BLOB, col_054 BLOB, col_055 BLOB,
    col_056 DECIMAL, col_057 DECIMAL, col_058 DECIMAL, col_059 DECIMAL, col_060 DECIMAL,
    col_061 SET<TEXT>, col_062 SET<INT>, col_063 LIST<TEXT>, col_064 LIST<INT>, col_065 MAP<TEXT, TEXT>,
    col_066 INET, col_067 INET, col_068 DATE, col_069 DATE, col_070 TIME,
    col_071 DURATION, col_072 TIMEUUID, col_073 VARCHAR, col_074 ASCII, col_075 TINYINT,
    col_076 SMALLINT, col_077 TEXT, col_078 TEXT, col_079 TEXT, col_080 TEXT,
    col_081 TEXT, col_082 TEXT, col_083 TEXT, col_084 TEXT, col_085 TEXT,
    col_086 TEXT, col_087 TEXT, col_088 TEXT, col_089 TEXT, col_090 TEXT,
    col_091 TEXT, col_092 TEXT, col_093 TEXT, col_094 TEXT, col_095 TEXT,
    col_096 TEXT, col_097 TEXT, col_098 TEXT, col_099 TEXT, col_100 TEXT
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Large blob storage table
CREATE TABLE IF NOT EXISTS large_blob_table (
    file_id UUID,
    chunk_id INT,
    file_name TEXT,
    mime_type TEXT,
    chunk_data BLOB,
    chunk_size INT,
    total_chunks INT,
    checksum TEXT,
    PRIMARY KEY (file_id, chunk_id)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND clustering ORDER BY (chunk_id ASC);

-- Chat messages with large text content
CREATE TABLE IF NOT EXISTS chat_messages (
    channel_id UUID,
    message_timestamp TIMESTAMP,
    message_id TIMEUUID,
    user_id UUID,
    username TEXT,
    message_content TEXT,
    attachments LIST<TEXT>,
    reactions MAP<TEXT, FROZEN<SET<UUID>>>,
    thread_id UUID,
    reply_count INT,
    edited_at TIMESTAMP,
    metadata MAP<TEXT, TEXT>,
    PRIMARY KEY (channel_id, message_timestamp, message_id)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'TimeWindowCompactionStrategy'}
  AND clustering ORDER BY (message_timestamp DESC, message_id DESC);

-- Document storage with versioning
CREATE TABLE IF NOT EXISTS document_versions (
    document_id UUID,
    version_number INT,
    created_at TIMESTAMP,
    author_id UUID,
    title TEXT,
    content TEXT,
    tags SET<TEXT>,
    metadata MAP<TEXT, TEXT>,
    word_count INT,
    character_count INT,
    change_summary TEXT,
    PRIMARY KEY (document_id, version_number)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND clustering ORDER BY (version_number DESC);

-- Product catalog with rich attributes
CREATE TABLE IF NOT EXISTS product_catalog (
    category_id UUID,
    product_id UUID,
    product_name TEXT,
    description TEXT,
    long_description TEXT,
    specifications MAP<TEXT, TEXT>,
    images LIST<TEXT>,
    tags SET<TEXT>,
    price DECIMAL,
    currency TEXT,
    availability_count INT,
    weight FLOAT,
    dimensions MAP<TEXT, FLOAT>,
    reviews_summary MAP<TEXT, DOUBLE>,
    attributes MAP<TEXT, FROZEN<SET<TEXT>>>,
    related_products SET<UUID>,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (category_id, product_id)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'};

-- Wide time series with many metrics per row
CREATE TABLE IF NOT EXISTS multi_metric_timeseries (
    device_id UUID,
    metric_timestamp TIMESTAMP,
    cpu_usage_percent FLOAT,
    memory_usage_bytes BIGINT,
    disk_io_read_bytes BIGINT,
    disk_io_write_bytes BIGINT,
    network_rx_bytes BIGINT,
    network_tx_bytes BIGINT,
    gpu_usage_percent FLOAT,
    gpu_memory_bytes BIGINT,
    temperature_celsius FLOAT,
    fan_speed_rpm INT,
    power_consumption_watts FLOAT,
    process_count INT,
    thread_count INT,
    handle_count INT,
    uptime_seconds BIGINT,
    load_average_1min FLOAT,
    load_average_5min FLOAT,
    load_average_15min FLOAT,
    disk_usage_percent FLOAT,
    swap_usage_bytes BIGINT,
    network_connections INT,
    active_sessions INT,
    error_count INT,
    warning_count INT,
    info_count INT,
    custom_metrics MAP<TEXT, DOUBLE>,
    status_flags SET<TEXT>,
    diagnostic_data BLOB,
    PRIMARY KEY (device_id, metric_timestamp)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': 2
  }
  AND clustering ORDER BY (metric_timestamp DESC);

-- Sparse wide table (many null values)
CREATE TABLE IF NOT EXISTS sparse_data_table (
    entity_id UUID,
    attribute_name TEXT,
    string_value TEXT,
    numeric_value DOUBLE,
    boolean_value BOOLEAN,
    timestamp_value TIMESTAMP,
    json_value TEXT,
    blob_value BLOB,
    set_value SET<TEXT>,
    list_value LIST<TEXT>,
    map_value MAP<TEXT, TEXT>,
    PRIMARY KEY (entity_id, attribute_name)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'};