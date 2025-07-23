#!/bin/bash
#
# Central script to manage the Cassandra test environment using a containerized data generator.
#
# Usage: ./manage.sh <command>
#
# Commands:
#   up            - Start the Cassandra container and wait for it to be healthy.
#   down          - Stop and remove the Cassandra container.
#   reset         - A full clean slate: stops the container and deletes all data volumes.
#   setup-schema  - Applies the CQL schema to the running database.
#   populate      - Builds and runs the containerized data generator to populate the database.
#   cqlsh         - Opens a CQL shell to the running container.
#   logs          - Tails the logs of the Cassandra container.
#   all           - Performs a full reset and brings up a populated database (reset, up, setup-schema, populate).

set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker/docker-compose.yml"
DATA_GENERATOR_DIR="$SCRIPT_DIR/data-generator"
CQL_SCHEMA_FILE="$SCRIPT_DIR/docker/create-keyspaces-fixed.cql"

# Docker-specific names
CASSANDRA_CONTAINER_NAME="cassandra-node1"
CASSANDRA_NETWORK_NAME="cassandra-network" # Should match the network in docker-compose.yml
GENERATOR_IMAGE_NAME="cqlite-data-generator"

# --- Helper Functions ---
function check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker could not be found. Please install Docker."
        exit 1
    fi
    if ! command -v docker-compose &> /dev/null; then
        echo "Error: docker-compose could not be found. Please install it."
        exit 1
    fi
}

function header() {
    echo ""
    echo "--- $1 ---"
    echo ""
}

# --- Commands ---
function up() {
    header "Starting Cassandra Environment"
    docker-compose -f "$COMPOSE_FILE" up -d
    echo "Waiting for Cassandra to be healthy..."
    while [ "$(docker inspect -f {{.State.Health.Status}} $CASSANDRA_CONTAINER_NAME 2>/dev/null)" != "healthy" ]; do
        if [ "$(docker inspect -f {{.State.Running}} $CASSANDRA_CONTAINER_NAME 2>/dev/null)" != "true" ]; then
            echo "Error: Cassandra container failed to start. Check logs with './manage.sh logs'"
            exit 1
        fi
        printf "."
        sleep 5
    done
    echo ""
    echo "Cassandra is up and healthy."
}

function down() {
    header "Stopping Cassandra Environment"
    docker-compose -f "$COMPOSE_FILE" down
}

function reset() {
    header "Resetting Cassandra Environment (Full Data Deletion)"
    docker-compose -f "$COMPOSE_FILE" down -v --remove-orphans
    echo "Environment stopped and data volumes removed."
}

function setup_schema() {
    header "Setting up Database Schema"
    if [ "$(docker inspect -f {{.State.Running}} $CASSANDRA_CONTAINER_NAME 2>/dev/null)" != "true" ]; then
        echo "Cassandra container is not running. Please run './manage.sh up' first."
        exit 1
    fi
    echo "Applying schema from $CQL_SCHEMA_FILE..."
    docker exec -i $CASSANDRA_CONTAINER_NAME cqlsh < "$CQL_SCHEMA_FILE"
    echo "Schema applied successfully."
}

function populate() {
    header "Populating Database with Containerized Data Generator"
    
    if [ "$(docker inspect -f {{.State.Running}} $CASSANDRA_CONTAINER_NAME 2>/dev/null)" != "true" ]; then
        echo "Cassandra container is not running. Please run './manage.sh up' first."
        exit 1
    fi

    echo "Building the data generator Docker image ($GENERATOR_IMAGE_NAME)..."
    docker build -t $GENERATOR_IMAGE_NAME "$DATA_GENERATOR_DIR"

    echo "Running the data generator container..."
    docker run --rm \
      --network=$CASSANDRA_NETWORK_NAME \
      -e CASSANDRA_HOST=$CASSANDRA_CONTAINER_NAME \
      -e CASSANDRA_PORT=9042 \
      $GENERATOR_IMAGE_NAME

    echo "Data population complete."
}

function cqlsh_shell() {
    header "Connecting to CQL Shell"
    docker exec -it $CASSANDRA_CONTAINER_NAME cqlsh
}

function extract_sstables() {
    header "Extracting SSTable data files from container"
    
    echo "Flushing memtables to SSTables on disk..."
    docker exec $CASSANDRA_CONTAINER_NAME nodetool flush test_keyspace
    echo "Flush command issued. Waiting a few seconds for files to be written..."
    sleep 5 # Give a moment for the flush to complete and files to be finalized.

    local source_path="/var/lib/cassandra/data/test_keyspace"
    local dest_path="$SCRIPT_DIR/sstables"

    echo "Removing old sstables directory if it exists..."
    rm -rf "$dest_path"
    mkdir -p "$dest_path"

    echo "Copying data from container ($CASSANDRA_CONTAINER_NAME:$source_path) to local machine ($dest_path)..."
    docker cp "$CASSANDRA_CONTAINER_NAME:$source_path" "$dest_path"
    
    # The files are copied into a subdirectory named 'test_keyspace'. Let's move them up.
    mv "$dest_path/test_keyspace"/* "$dest_path"/
    rmdir "$dest_path/test_keyspace"
    
    echo "SSTable files extracted successfully to: $dest_path"
    echo "Contents:"
    ls -l "$dest_path"
}

function logs() {
    header "Tailing Cassandra Logs"
    docker-compose -f "$COMPOSE_FILE" logs -f
}

# --- Main Logic ---
check_docker

COMMAND=$1
if [ -z "$COMMAND" ]; then
    echo "Usage: $0 <command>"
    echo "Available commands: up, down, reset, setup-schema, populate, cqlsh, logs, extract-sstables, all"
    exit 1
fi

case "$COMMAND" in
    up) up ;;
    down) down ;;
    reset) reset ;;
    setup-schema) setup_schema ;;
    populate) populate ;;
    cqlsh) cqlsh_shell ;;
    logs) logs ;;
    extract-sstables) extract_sstables ;;
    all)
        reset
        up
        setup_schema
        populate
        ;;
    *)
        echo "Unknown command: $COMMAND"
        exit 1
        ;;
esac

echo ""
echo "Command '$COMMAND' completed successfully." 