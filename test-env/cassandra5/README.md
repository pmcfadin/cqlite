# Cassandra 5 Test Data Generation Framework

This directory contains a self-contained, reproducible framework for generating realistic and scalable test data for Apache Cassandra 5.0. It is designed to be used by developers and automated AI agents to create datasets for testing, validation, and performance analysis.

The framework uses a containerized approach, leveraging Docker and Docker Compose to ensure a consistent environment across any machine.

## Prerequisites

- **Docker**: The framework requires Docker to be installed and running.
- **Docker Compose**: The `docker-compose` command must be available.

## Directory Structure

Here is an overview of the important files and directories:

```
test-env/cassandra5/
├── manage.sh                  # The main control script for the entire framework.
├── docker/
│   ├── docker-compose.yml     # Defines the Cassandra 5.0 service.
│   └── create-keyspaces-fixed.cql # The master CQL schema file.
├── data-generator/
│   ├── Dockerfile             # Defines the container environment for the Python data generator.
│   ├── requirements.txt       # Python dependencies (cassandra-driver, Faker).
│   ├── config.py              # **Main configuration for data size.**
│   ├── schemas.py             # **Python-based definitions of the database schema.**
│   ├── generator.py           # The core data generation logic.
│   └── populate.py            # The main script that runs the data population.
└── sstables/
    └── ... (This directory is created by the 'extract-sstables' command)
```

## Configuration

The data generation process is highly configurable without needing to alter the core logic.

### 1. Adjusting Data Volume

To change how many rows are generated for each table, edit the `ROW_COUNTS` dictionary in `data-generator/config.py`.

**Example:**
```python
# data-generator/config.py

ROW_COUNTS = {
    # P0 - Critical Tables
    "all_types": 1000,          # Generate 1,000 rows for the all_types table
    "collections_table": 1000,
    "users": 500,
    "large_table": 50_000,      # Increase to 50,000 for a larger test
    ...
}
```

### 2. Modifying the Schema

The framework is designed to be adaptable to schema changes. The process involves two steps:

1.  **Update the CQL Schema**: Modify the `CREATE TABLE` statements in `docker/create-keyspaces-fixed.cql`.
2.  **Update the Python Schema**: Modify the Python dictionary definitions in `data-generator/schemas.py` to match the changes made in the CQL file. The generator uses these Python definitions to understand what kind of data to create.

## Usage with `manage.sh`

The `manage.sh` script is the primary interface for controlling the test environment.

**First-time setup:** Make the script executable:
```bash
chmod +x ./manage.sh
```

### Available Commands

-   **`./manage.sh up`**
    Starts the Cassandra container and waits for it to become healthy.

-   **`./manage.sh down`**
    Stops the Cassandra container.

-   **`./manage.sh reset`**
    Performs a full reset by stopping the container and **deleting all data volumes**. This is the cleanest way to start fresh.

-   **`./manage.sh setup-schema`**
    Applies the schema from `docker/create-keyspaces-fixed.cql` to the running Cassandra instance.

-   **`./manage.sh populate`**
    Builds the data generator Docker image and runs a container to populate the database with test data based on the current configuration.

-   **`./manage.sh cqlsh`**
    Opens an interactive CQL shell to the running Cassandra container, allowing you to manually inspect the data.

-   **`./manage.sh extract-sstables`**
    Flushes all in-memory data to disk and copies the resulting SSTable files from the container to the local `test-env/cassandra5/sstables/` directory.

-   **`./manage.sh all`**
    The most common command for a full workflow. It performs a `reset`, `up`, `setup-schema`, and `populate` all in one step.

---

## Standard Workflow: Generating and Extracting Data

For most use cases, the goal is to get a fresh set of SSTable files. The workflow is simple:

**Step 1: Create and Populate the Database**

Run the `all` command to build the environment and generate the data from scratch.

```bash
./manage.sh all
```
*Expected Output:* The script will show the progress of resetting the environment, starting Cassandra, applying the schema, and finally, the log output from the data generator as it populates each table.

**Step 2: Extract the SSTable Files**

After the `all` command completes, run the `extract-sstables` command.

```bash
./manage.sh extract-sstables
```
*Expected Output:* The script will first show that it is flushing the memtables. Then, it will show the `docker cp` operation and finish by listing the contents of the newly created `sstables/` directory, which will contain a folder for each table.

```
--- Extracting SSTable data files from container ---

Flushing memtables to SSTables on disk...
Flush command issued. Waiting a few seconds for files to be written...
Removing old sstables directory if it exists...
Copying data from container (cassandra-node1:/var/lib/cassandra/data/test_keyspace) to local machine (...)
SSTable files extracted successfully to: .../test-env/cassandra5/sstables
Contents:
... (a list of table directories will appear here) ...
```
The SSTable files are now available in `test-env/cassandra5/sstables/` for validation and use. 