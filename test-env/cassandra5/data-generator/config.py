# --- CASSANDRA CONNECTION SETTINGS ---
# The contact points should be the hostname of the Cassandra container.
# This will be passed in as an environment variable from our manage script.
import os

CASSANDRA_HOSTS = [os.environ.get("CASSANDRA_HOST", "127.0.0.1")]
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
KEYSPACE = "test_keyspace"


# --- DATA GENERATION SETTINGS ---

# This seed ensures that Faker generates the same data every time the script is run.
# This is critical for creating reproducible test datasets.
FAKER_SEED = 1234

# --- TABLE-SPECIFIC ROW COUNTS ---
# Define how many rows to generate for each table. This makes it easy to
# configure different test scenarios (e.g., small, medium, large).

ROW_COUNTS = {
    # P0 - Critical Tables
    "all_types": 5000,          # For primitive type validation - increased for comprehensive testing
    "collections_table": 3000,  # For collection type validation - varied collection sizes
    "users": 2000,              # For UDT validation - increased for better coverage
    "large_table": 50_000,      # For scale and performance testing - 5x increase

    # P1 - High Priority Tables
    "time_series": 10_000,      # For time-series data patterns - doubled for better partitioning
    "multi_clustering": 2000,   # For complex key validation

    # P2 - Medium Priority Tables
    "static_test": 100,         # For static column validation
    "counters": 50,             # For counter column validation
}


# --- DATA GENERATION BEHAVIOR ---

# Number of rows to insert in a single batch.
# Tuning this can affect insert performance.
BATCH_SIZE = 100

# For collection types, this controls the variety of sizes.
# The number of elements in a list, set, or map will be between these values.
MIN_COLLECTION_ELEMENTS = 0
MAX_COLLECTION_ELEMENTS = 10

# For wide tables, define the number of columns to generate
WIDE_TABLE_COLUMN_COUNT = 50

# For time-series data, define the time range for generated events.
TIME_SERIES_START_DATE = "2023-01-01 00:00:00"
TIME_SERIES_END_DATE = "2024-01-01 00:00:00"
TIME_SERIES_PARTITIONS = ["sensor-alpha", "sensor-beta", "sensor-gamma", "sensor-delta"]

# --- LOGGING AND OUTPUT ---
LOG_LEVEL = "INFO"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s' 