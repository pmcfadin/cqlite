#!/usr/bin/env bash
# Regenerate the server-emitted Arrow IPC golden used by ArrowToTrinoGoldenTest
# (issue #2234).
#
# The golden bytes MUST come from the real cqlite-flight emission path — a
# `MergeProducer` over a real SSTable fixture, serialized with the server's wire
# Arrow schema (uuid extension metadata, Timestamp(Millisecond,"UTC") unit,
# Date32, and the cqlite:pushdown field metadata). This script drives the Rust
# `emit_arrow_golden` example so updating the golden is deliberate and never a
# hand-edit.
#
# Usage: trino-connector/scripts/regen-arrow-golden.sh
# Run it whenever the server's Arrow schema/type mapping changes (which is
# exactly the drift ArrowToTrinoGoldenTest guards). Commit the regenerated blob
# with `git add -f` (it lives under a resources dir but is a tracked fixture).
set -euo pipefail

# Repo root = two levels up from this script (trino-connector/scripts).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUT="${REPO_ROOT}/trino-connector/src/test/resources/golden/all_scalars.arrows"

echo "Regenerating ${OUT} from the cqlite-flight server emission path..."
cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" \
    -p cqlite-flight --example emit_arrow_golden -- "${OUT}"

echo "Done. Review the diff and commit with: git add -f ${OUT}"
