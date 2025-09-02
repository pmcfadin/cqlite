#!/bin/bash
# Docker-based sstabledump wrapper for Issue #38 parity tests
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

# Ensure Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is required but not available" >&2
    exit 1
fi

# Resolve absolute path
if [[ "$SSTABLE_PATH" != /* ]]; then
    SSTABLE_PATH="$(pwd)/$SSTABLE_PATH"
fi

# Extract directory containing the SSTable
SSTABLE_DIR="$(dirname "$SSTABLE_PATH")"
SSTABLE_FILE="$(basename "$SSTABLE_PATH")"

echo "🔍 Running sstabledump via Cassandra 5.0 Docker container..." >&2
echo "📂 SSTable: $SSTABLE_PATH" >&2

# Run sstabledump in Cassandra container
docker run --rm \
    -v "$SSTABLE_DIR:/sstable" \
    cassandra:5.0 \
    sstabledump $ADDITIONAL_ARGS "/sstable/$SSTABLE_FILE"

echo "✅ sstabledump completed successfully" >&2