USE test_keyspace;

-- Generate test data for all_types table
INSERT INTO all_types (
  id, text_col, ascii_col, varchar_col, bigint_col, blob_col, boolean_col,
  date_col, decimal_col, double_col, duration_col, float_col, inet_col,
  int_col, smallint_col, time_col, timestamp_col, timeuuid_col, tinyint_col, varint_col
) VALUES (
  uuid(), 'Sample text', 'ASCII text', 'VARCHAR text', 9223372036854775807,
  textAsBlob('Binary data'), true, '2023-01-01', 123.456, 3.14159,
  'P1Y2M3DT4H5M6S', 2.71828, '192.168.1.1', 2147483647, 32767,
  '12:34:56.789', '2023-01-01 12:34:56.789+0000', now(), 127, 12345678901234567890
);

-- More test data with different values
INSERT INTO all_types (
  id, text_col, ascii_col, varchar_col, bigint_col, blob_col, boolean_col,
  date_col, decimal_col, double_col, duration_col, float_col, inet_col,
  int_col, smallint_col, time_col, timestamp_col, timeuuid_col, tinyint_col, varint_col
) VALUES (
  uuid(), 'Another text', 'More ASCII', 'More VARCHAR', -9223372036854775808,
  textAsBlob('More binary'), false, '2023-12-31', -999.999, -3.14159,
  'PT1H30M', -2.71828, '10.0.0.1', -2147483648, -32768,
  '23:59:59.999', '2023-12-31 23:59:59.999+0000', now(), -128, -98765432109876543210
);

-- Generate data for collections table
INSERT INTO collections_table (
  id, list_col, set_col, map_col, frozen_list, frozen_set, frozen_map
) VALUES (
  uuid(), 
  ['item1', 'item2', 'item3'], 
  {1, 2, 3, 4, 5}, 
  {'key1': 10, 'key2': 20, 'key3': 30},
  ['frozen1', 'frozen2'], 
  {100, 200, 300}, 
  {'fkey1': 1000, 'fkey2': 2000}
);

-- Generate data for users table with UDTs
INSERT INTO users (
  id, profile, addresses, metadata
) VALUES (
  uuid(),
  {
    name: 'John Doe',
    age: 30,
    address: {
      street: '123 Main St',
      city: 'Anytown',
      state: 'CA',
      zip_code: '12345'
    }
  },
  [
    {
      street: '456 Oak Ave',
      city: 'Otherville',
      state: 'NY',
      zip_code: '67890'
    }
  ],
  {'department': 'Engineering', 'level': 'Senior'}
);

-- Generate time series data
INSERT INTO time_series (partition_key, timestamp, value, tags) VALUES 
  ('sensor1', '2023-01-01 00:00:00+0000', 23.5, {'location': 'room1', 'type': 'temperature'}),
  ('sensor1', '2023-01-01 00:05:00+0000', 24.1, {'location': 'room1', 'type': 'temperature'}),
  ('sensor1', '2023-01-01 00:10:00+0000', 23.8, {'location': 'room1', 'type': 'temperature'}),
  ('sensor2', '2023-01-01 00:00:00+0000', 60.2, {'location': 'room2', 'type': 'humidity'}),
  ('sensor2', '2023-01-01 00:05:00+0000', 61.5, {'location': 'room2', 'type': 'humidity'});

-- Generate multi-clustering data
INSERT INTO multi_clustering (pk, ck1, ck2, ck3, data) VALUES
  ('partition1', 'cluster1', 100, '2023-01-01 12:00:00+0000', 'data1'),
  ('partition1', 'cluster1', 200, '2023-01-01 13:00:00+0000', 'data2'),
  ('partition1', 'cluster2', 150, '2023-01-01 14:00:00+0000', 'data3'),
  ('partition2', 'cluster1', 300, '2023-01-01 15:00:00+0000', 'data4');

-- Generate large table data (create multiple records)
INSERT INTO large_table (id, data, metadata, created_at) VALUES
  (uuid(), 'Large data record 1', textAsBlob('metadata1'), dateof(now())),
  (uuid(), 'Large data record 2', textAsBlob('metadata2'), dateof(now())),
  (uuid(), 'Large data record 3', textAsBlob('metadata3'), dateof(now()));

-- Generate counter data
UPDATE counters SET view_count = view_count + 100, like_count = like_count + 25, share_count = share_count + 5 WHERE id = 'post1';
UPDATE counters SET view_count = view_count + 250, like_count = like_count + 75, share_count = share_count + 15 WHERE id = 'post2';

-- Generate static columns data
INSERT INTO static_test (partition_key, clustering_key, static_data, regular_data) VALUES
  ('partition1', 'row1', 'Static data for partition1', 'Regular data 1'),
  ('partition1', 'row2', 'Static data for partition1', 'Regular data 2'),
  ('partition2', 'row1', 'Static data for partition2', 'Regular data 3');

-- Generate more diverse data for testing edge cases
INSERT INTO all_types (
  id, text_col, bigint_col, blob_col, boolean_col, int_col
) VALUES (
  uuid(), '', 0, 0x, false, 0
);

-- Insert NULL values
INSERT INTO all_types (id, text_col) VALUES (uuid(), NULL);

-- Large text data
INSERT INTO all_types (id, text_col, blob_col) VALUES (
  uuid(), 
  'This is a very long text string that should test the limits of text storage in Cassandra. It contains many words and characters to ensure proper handling of larger text fields. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.',
  textAsBlob('This is a large binary blob with lots of data that should test blob storage capabilities')
);

-- Test special characters and Unicode
INSERT INTO all_types (id, text_col, varchar_col) VALUES (
  uuid(),
  'Special chars: !@#$%^&*()_+{}|:"<>?[]\\;'',./',
  'Unicode: 你好世界 🌍 emoji test ñáéíóú'
);