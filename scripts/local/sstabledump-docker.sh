#!/bin/bash
# Container-based sstabledump wrapper for Issue #38 parity tests (Podman/Docker)
# Provides real Cassandra sstabledump output instead of placeholder fallback

set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <sstable-path> [additional-args...]"
    echo "Example: $0 /path/to/nb-1-big-Data.db -d"
    exit 1
fi

SSTABLE_PATH="$1"
shift
ADDITIONAL_ARGS="$@"

# Select container engine: prefer podman, fallback to docker
ENGINE="${CONTAINER_ENGINE:-}"
if [[ -z "$ENGINE" ]]; then
    if command -v podman >/dev/null 2>&1; then
        ENGINE="podman"
    elif command -v docker >/dev/null 2>&1; then
        ENGINE="docker"
    else
        echo "❌ Neither podman nor docker is available" >&2
        exit 1
    fi
fi

# Resolve absolute path
if [[ "$SSTABLE_PATH" != /* ]]; then
    SSTABLE_PATH="$(pwd)/$SSTABLE_PATH"
fi

# Extract directory containing the SSTable
SSTABLE_DIR="$(dirname "$SSTABLE_PATH")"
SSTABLE_FILE="$(basename "$SSTABLE_PATH")"

echo "🔍 Running sstabledump via Cassandra 5.0 $ENGINE container..." >&2
echo "📂 SSTable: $SSTABLE_PATH" >&2

# Run sstabledump in Cassandra container
# Prefer absolute tool path used in Cassandra images
TOOL_PATH="/opt/cassandra/tools/bin/sstabledump"

$ENGINE run --rm \
    -v "$SSTABLE_DIR:/sstable" \
    docker.io/library/cassandra:5.0 \
    bash -lc "\"$TOOL_PATH\" \"/sstable/$SSTABLE_FILE\" $ADDITIONAL_ARGS || (command -v sstabledump >/dev/null 2>&1 && sstabledump \"/sstable/$SSTABLE_FILE\" $ADDITIONAL_ARGS)"

echo "✅ sstabledump completed successfully" >&2