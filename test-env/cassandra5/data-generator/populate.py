import logging
import time
from cassandra.cluster import Cluster, BatchStatement
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchType, PreparedStatement
from cassandra.concurrent import execute_concurrent_with_args

import config
import schemas
import generator

# --- Logging Setup ---
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
log = logging.getLogger(__name__)


def register_udts(cluster, session):
    """
    Manually registers all UDTs defined in schemas.py with the Cassandra driver.
    Handles nested UDTs by ensuring base UDTs are registered before composite ones.
    Returns a dictionary of registered UDT classes for the generator to use.
    """
    log.info("Registering UDTs...")
    session.set_keyspace(config.KEYSPACE)
    
    registered_udts = {}
    
    # Simple dependency resolution: register UDTs that don't contain other UDTs first.
    udt_definitions = schemas.UDTS.copy()
    
    while len(registered_udts) < len(udt_definitions):
        udts_registered_in_pass = 0
        for name, fields in udt_definitions.items():
            if name in registered_udts:
                continue

            dependencies = [
                field_type[1] for field_type in fields.values() 
                if isinstance(field_type, tuple) and 'udt' in field_type[0]
            ]
            
            if all(dep in registered_udts for dep in dependencies):
                field_names = list(fields.keys())
                log.debug(f"Registering UDT '{name}' with fields: {field_names}")
                
                # Define an __init__ method for the dynamic class.
                # This allows the generator to instantiate it with keyword arguments.
                def __init__(self, **kwargs):
                    for key, value in kwargs.items():
                        setattr(self, key, value)

                # Dynamically create a class with the correct attributes and __init__ method.
                class_attributes = {field: None for field in field_names}
                class_attributes['__init__'] = __init__
                udt_class = type(name, (object,), class_attributes)
                
                # Register the dynamically created class.
                cluster.register_user_type(config.KEYSPACE, name, udt_class)
                
                # Store the class so the generator can create instances of it.
                registered_udts[name] = udt_class
                udts_registered_in_pass += 1

        if udts_registered_in_pass == 0 and len(registered_udts) < len(udt_definitions):
            unregistered = set(udt_definitions.keys()) - set(registered_udts.keys())
            raise RuntimeError(f"Could not resolve UDT dependencies. Unregistered UDTs: {unregistered}")
            
    log.info("All UDTs registered successfully.")
    return registered_udts


def connect_to_cassandra():
    """Establishes and tests the connection to Cassandra."""
    log.info(f"Connecting to Cassandra at {config.CASSANDRA_HOSTS}:{config.CASSANDRA_PORT}...")
    cluster = Cluster(contact_points=config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
    
    retries = 5
    for i in range(retries):
        try:
            session = cluster.connect()
            log.info("Successfully connected to Cassandra.")
            return cluster, session
        except Exception as e:
            log.warning(f"Connection attempt {i+1}/{retries} failed: {e}")
            if i < retries - 1:
                time.sleep(5)
            else:
                log.error("Could not connect to Cassandra after several retries.")
                raise

def populate_table(session, table_name, registered_udts):
    """
    Generates and inserts data for a single table using concurrent, asynchronous executions.
    This is the recommended pattern for high-throughput bulk loading.
    """
    num_rows = config.ROW_COUNTS.get(table_name, 0)
    if num_rows == 0:
        log.info(f"Skipping table {table_name} as row count is 0.")
        return

    log.info(f"Populating table '{table_name}' with {num_rows} rows...")
    
    query = schemas.get_insert_query(table_name)
    prepared_statement = session.prepare(query)

    log.info(f"  ... Generating {num_rows} parameter sets...")
    parameters = [
        generator.generate_row_data(table_name, registered_udts)
        for _ in range(num_rows)
    ]

    log.info(f"  ... Executing inserts concurrently...")
    results = execute_concurrent_with_args(
        session, 
        prepared_statement, 
        parameters, 
        concurrency=100, # This can be tuned based on performance
        raise_on_first_error=False
    )
    
    # Check for errors
    errors = 0
    for success, result_or_exc in results:
        if not success:
            errors += 1
            log.error(f"Failed to insert row: {result_or_exc}")

    if errors > 0:
        log.warning(f"Completed insert for table '{table_name}' with {errors} errors.")
    else:
        log.info(f"Successfully inserted {num_rows} rows into table '{table_name}'.")

def populate_counters(session):
    """Handles populating the counter table using concurrent, asynchronous executions."""
    table_name = "counters"
    num_rows = config.ROW_COUNTS.get(table_name, 0)
    if num_rows == 0:
        return
        
    log.info(f"Populating table '{table_name}' with {num_rows} counter updates...")
    
    query = "UPDATE counters SET view_count = view_count + ?, like_count = like_count + ?, share_count = share_count + ? WHERE id = ?"
    prepared = session.prepare(query)

    parameters = [
        (
            generator.faker.random_int(min=100, max=1000),
            generator.faker.random_int(min=5, max=100),
            generator.faker.random_int(min=1, max=20),
            f"post_{i+1}"
        )
        for i in range(num_rows)
    ]
    
    results = execute_concurrent_with_args(
        session,
        prepared,
        parameters,
        concurrency=100,
        raise_on_first_error=False
    )

    errors = 0
    for success, result_or_exc in results:
        if not success:
            errors += 1
            log.error(f"Failed to update counter: {result_or_exc}")
            
    if errors > 0:
        log.warning(f"Completed counter updates for table '{table_name}' with {errors} errors.")
    else:
        log.info(f"Successfully updated {num_rows} counter rows.")


def main():
    """Main function to orchestrate the data population process."""
    cluster, session = None, None
    try:
        cluster, session = connect_to_cassandra()
        registered_udts = register_udts(cluster, session)
        
        log.info(f"Starting data generation for keyspace '{config.KEYSPACE}'.")
        
        for table_name in schemas.TABLES.keys():
            populate_table(session, table_name, registered_udts)
            
        populate_counters(session)
        
        log.info("Data population completed successfully.")

    except Exception as e:
        log.critical(f"A critical error occurred: {e}", exc_info=True)
    finally:
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()
        log.info("Cassandra connection closed.")


if __name__ == "__main__":
    main() 