
                CREATE KEYSPACE IF NOT EXISTS compression_test_zstd 
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
                
                USE compression_test_zstd;
                
                CREATE TABLE IF NOT EXISTS test_table (
                    id int PRIMARY KEY,
                    data text,
                    value double,
                    timestamp timestamp
                ) WITH compression = {
                    'class': 'org.apache.cassandra.io.compress.ZstdCompressor',
                    'chunk_length_in_kb': 64
                };
                
                -- Insert test data
                INSERT INTO test_table (id, data, value, timestamp) 
                VALUES (1, 'Test data for compression validation', 123.456, toTimestamp(now()));
                
                INSERT INTO test_table (id, data, value, timestamp) 
                VALUES (2, 'Another test row with different data', 789.012, toTimestamp(now()));
                
                INSERT INTO test_table (id, data, value, timestamp) 
                VALUES (3, 'Third row for chunk boundary testing', 345.678, toTimestamp(now()));
                