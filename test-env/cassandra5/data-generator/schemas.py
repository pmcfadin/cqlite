import config

# --- User Defined Types (UDTs) ---
# UDTs are now defined as dictionaries. This makes them easy to inspect
# and decouples them from any ORM. The generator will use these
# definitions to build nested data.
UDTS = {
    'address': {
        'street': 'text',
        'city': 'text',
        'state': 'text',
        'zip_code': 'text'
    },
    'person': {
        'name': 'text',
        'age': 'int',
        'address': ('udt', 'address') # This indicates a nested UDT
    }
}


# --- Table Schemas ---
# Tables are now defined in a dictionary. The keys are column names,
# and the values are strings representing the data type for the generator.
# For complex types like collections or UDTs, a tuple is used.
TABLES = {
    "all_types": {
        'id': 'uuid', 'text_col': 'text', 'ascii_col': 'ascii', 'varchar_col': 'text',
        'bigint_col': 'bigint', 'blob_col': 'blob', 'boolean_col': 'boolean',
        'date_col': 'date', 'decimal_col': 'decimal', 'double_col': 'double',
        'duration_col': 'duration', 'float_col': 'float', 'inet_col': 'inet',
        'int_col': 'int', 'smallint_col': 'smallint', 'time_col': 'time',
        'timestamp_col': 'timestamp', 'timeuuid_col': 'timeuuid',
        'tinyint_col': 'tinyint', 'varint_col': 'varint'
    },
    "collections_table": {
        'id': 'uuid',
        'list_col': ('list', 'text'),
        'set_col': ('set', 'int'),
        'map_col': ('map', 'text', 'int'),
        'frozen_list': ('frozen_list', 'text'),
        'frozen_set': ('frozen_set', 'int'),
        'frozen_map': ('frozen_map', 'text', 'int')
    },
    "users": {
        'id': 'uuid',
        'profile': ('frozen_udt', 'person'),
        'addresses': ('list', ('frozen_udt', 'address')),
        'metadata': ('map', 'text', 'text')
    },
    "time_series": {
        'partition_key': 'text',
        'timestamp': 'timestamp',
        'value': 'double',
        'tags': ('map', 'text', 'text')
    },
    "multi_clustering": {
        'pk': 'text',
        'ck1': 'text',
        'ck2': 'int',
        'ck3': 'timestamp',
        'data': 'text'
    },
    "large_table": {
        'id': 'uuid',
        'data': 'text',
        'metadata': 'blob',
        'created_at': 'timestamp'
    },
    "static_test": {
        'partition_key': 'text',
        'clustering_key': 'text',
        'static_data': 'text',
        'regular_data': 'text'
    },
    # Counter tables are handled separately in populate.py
}


def get_insert_query(table_name):
    """
    Generates a CQL INSERT statement string for a given table.
    This is still adaptable and now reads from the dictionary-based schema.
    """
    if table_name not in TABLES:
        raise ValueError(f"Unknown table: {table_name}")

    model = TABLES[table_name]
    columns = model.keys()

    column_names = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])

    return f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})" 