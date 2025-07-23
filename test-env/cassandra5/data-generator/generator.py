import random
import uuid
from faker import Faker
from datetime import datetime, date, time, timedelta
from decimal import Decimal
import ipaddress
from cassandra.util import Duration
import schemas
import config

# Initialize Faker with a seed for reproducibility
faker = Faker()
Faker.seed(config.FAKER_SEED)

def generate_primitive(data_type):
    """Generates data for a single primitive type."""
    return PRIMITIVE_GENERATORS.get(data_type, lambda: None)()

def generate_udt_data(udt_name, registered_udts):
    """
    Generates a dictionary of data for a given UDT name.
    It now uses the registered UDT class from the driver.
    """
    udt_definition = schemas.UDTS.get(udt_name)
    if not udt_definition:
        raise ValueError(f"Unknown UDT schema: {udt_name}")
    
    udt_data = {}
    for field_name, field_type in udt_definition.items():
        udt_data[field_name] = generate_data_for_type(field_type, registered_udts)
    
    # Use the registered UDT class to create an instance
    udt_class = registered_udts.get(udt_name)
    return udt_class(**udt_data) if udt_class else None

def generate_collection_data(collection_tuple, registered_udts):
    """Generates data for collection types based on a tuple definition."""
    collection_type = collection_tuple[0]
    sub_type = collection_tuple[1]
    
    min_items = config.MIN_COLLECTION_ELEMENTS
    max_items = config.MAX_COLLECTION_ELEMENTS
    num_items = random.randint(min_items, max_items)
    
    if collection_type in ('list', 'set', 'frozen_list', 'frozen_set'):
        items = [generate_data_for_type(sub_type, registered_udts) for _ in range(num_items)]
        return set(items) if 'set' in collection_type else items
        
    elif collection_type in ('map', 'frozen_map'):
        key_type = sub_type
        value_type = collection_tuple[2]
        return {
            generate_data_for_type(key_type, registered_udts): generate_data_for_type(value_type, registered_udts)
            for _ in range(num_items)
        }
    return None

def generate_data_for_type(type_def, registered_udts):
    """
    Recursive function to generate data for any defined type,
    including primitives, collections, and UDTs.
    """
    if isinstance(type_def, str):
        return generate_primitive(type_def)
    elif isinstance(type_def, tuple):
        type_name = type_def[0]
        if 'udt' in type_name:
            return generate_udt_data(type_def[1], registered_udts)
        elif type_name in ('list', 'set', 'map', 'frozen_list', 'frozen_set', 'frozen_map'):
            return generate_collection_data(type_def, registered_udts)
    raise TypeError(f"Unknown type definition: {type_def}")


def generate_row_data(table_name, registered_udts):
    """
    Generates a dictionary of data for a single row of a given table.
    Now passes the registered UDTs from the driver down the call stack.
    """
    table_schema = schemas.TABLES.get(table_name)
    if not table_schema:
        raise ValueError(f"Unknown table schema: {table_name}")

    row_data = {}
    for col_name, col_type in table_schema.items():
        row_data[col_name] = generate_data_for_type(col_type, registered_udts)

    # Override specific fields for more realistic scenarios
    if table_name == "time_series":
        row_data["partition_key"] = random.choice(config.TIME_SERIES_PARTITIONS)
    
    if table_name == "large_table":
        row_data["data"] = faker.text(max_nb_chars=2000)
        row_data["metadata"] = faker.binary(length=random.randint(1024, 4096))
        
    return row_data

# --- Primitive Type Generators ---
PRIMITIVE_GENERATORS = {
    'uuid': uuid.uuid4,
    'text': lambda: faker.text(max_nb_chars=100),
    'ascii': lambda: faker.pystr(min_chars=10, max_chars=50),
    'bigint': faker.pyint,
    'blob': lambda: faker.binary(length=random.randint(10, 200)),
    'boolean': faker.pybool,
    'date': faker.date_object,
    'decimal': faker.pydecimal,
    'double': faker.pyfloat,
    'duration': lambda: Duration(
        months=faker.random_int(min=0, max=24),
        days=faker.random_int(min=0, max=30),
        nanoseconds=faker.random_int(min=0, max=999999999)
    ),
    'float': lambda: float(faker.pyfloat()),
    'inet': faker.ipv4,
    'int': faker.pyint,
    'smallint': lambda: faker.random_int(min=-32768, max=32767),
    'time': faker.time_object,
    'timestamp': lambda: faker.date_time_between(start_date=datetime.fromisoformat(config.TIME_SERIES_START_DATE), end_date=datetime.fromisoformat(config.TIME_SERIES_END_DATE)),
    'timeuuid': uuid.uuid1,
    'tinyint': lambda: faker.random_int(min=-128, max=127),
    'varint': faker.pyint,
} 