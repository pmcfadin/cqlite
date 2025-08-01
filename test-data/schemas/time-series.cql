-- Time Series Schema for CQL Test Data Generation
-- Optimized for time-based data with TTLs and time window compaction
-- Issue #18: Docker-based test data generation

-- Create keyspace for time series testing
CREATE KEYSPACE IF NOT EXISTS test_timeseries WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
} AND durable_writes = true;

USE test_timeseries;

-- IoT sensor data table
CREATE TABLE IF NOT EXISTS sensor_data (
    sensor_id UUID,
    timestamp TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    pressure DOUBLE,
    battery_level TINYINT,
    location TEXT,
    status TEXT,
    PRIMARY KEY (sensor_id, timestamp)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': 1
  }
  AND clustering ORDER BY (timestamp DESC);

-- Application metrics with TTL
CREATE TABLE IF NOT EXISTS app_metrics (
    application_id TEXT,
    metric_name TEXT,
    timestamp TIMESTAMP,
    value DOUBLE,
    unit TEXT,
    tags MAP<TEXT, TEXT>,
    PRIMARY KEY ((application_id, metric_name), timestamp)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'MINUTES',
    'compaction_window_size': 30
  }
  AND clustering ORDER BY (timestamp DESC)
  AND default_time_to_live = 2592000; -- 30 days

-- User activity tracking
CREATE TABLE IF NOT EXISTS user_activity (
    user_id UUID,
    activity_date DATE,
    activity_time TIMESTAMP,
    activity_type TEXT,
    page_url TEXT,
    session_id UUID,
    duration_ms INT,
    metadata MAP<TEXT, TEXT>,
    PRIMARY KEY ((user_id, activity_date), activity_time)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
  }
  AND clustering ORDER BY (activity_time DESC);

-- Financial time series data
CREATE TABLE IF NOT EXISTS stock_prices (
    symbol TEXT,
    trading_day DATE,
    timestamp TIMESTAMP,
    open_price DECIMAL,
    high_price DECIMAL,
    low_price DECIMAL,
    close_price DECIMAL,
    volume BIGINT,
    adjusted_close DECIMAL,
    PRIMARY KEY ((symbol, trading_day), timestamp)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 7
  }
  AND clustering ORDER BY (timestamp ASC);

-- Log aggregation table
CREATE TABLE IF NOT EXISTS log_entries (
    service_name TEXT,
    log_level TEXT,
    hour_bucket TIMESTAMP,
    log_id TIMEUUID,
    message TEXT,
    source_file TEXT,
    line_number INT,
    thread_name TEXT,
    correlation_id UUID,
    stack_trace TEXT,
    PRIMARY KEY ((service_name, log_level, hour_bucket), log_id)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': 6
  }
  AND clustering ORDER BY (log_id DESC)
  AND default_time_to_live = 604800; -- 7 days

-- Event sourcing table
CREATE TABLE IF NOT EXISTS event_store (
    aggregate_id UUID,
    version BIGINT,
    event_id TIMEUUID,
    event_type TEXT,
    event_data TEXT,
    metadata MAP<TEXT, TEXT>,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'}
  AND clustering ORDER BY (version ASC);

-- Time bucketed counters
CREATE TABLE IF NOT EXISTS time_bucketed_counters (
    metric_name TEXT,
    time_bucket TIMESTAMP,
    total_count COUNTER,
    error_count COUNTER,
    success_count COUNTER,
    PRIMARY KEY (metric_name, time_bucket)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'MINUTES',
    'compaction_window_size': 5
  }
  AND clustering ORDER BY (time_bucket DESC);

-- Session tracking with variable TTL
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id UUID PRIMARY KEY,
    user_id UUID,
    start_time TIMESTAMP,
    last_activity TIMESTAMP,
    ip_address INET,
    user_agent TEXT,
    device_info MAP<TEXT, TEXT>,
    is_active BOOLEAN
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- High-frequency trading data
CREATE TABLE IF NOT EXISTS tick_data (
    symbol TEXT,
    exchange TEXT,
    minute_bucket TIMESTAMP,
    tick_id TIMEUUID,
    price DECIMAL,
    volume INT,
    bid_price DECIMAL,
    ask_price DECIMAL,
    trade_type TEXT,
    PRIMARY KEY ((symbol, exchange, minute_bucket), tick_id)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'MINUTES',
    'compaction_window_size': 1
  }
  AND clustering ORDER BY (tick_id ASC)
  AND default_time_to_live = 86400; -- 1 day