#!/usr/bin/env bash
# Regenerate the server-emitted goldens used by the connector's Flight decode
# tests. BOTH come from the real cqlite-flight emission path — never a hand-edit.
#
#   all_scalars.arrows   (issue #2234) — an Arrow IPC STREAM (StreamWriter) over
#     a MergeProducer fixture covering every scalar CQL type, serialized with the
#     server's wire Arrow schema (uuid extension, Timestamp(Millisecond,"UTC")
#     unit, Date32, cqlite:pushdown field metadata). Decoded by
#     ArrowToTrinoGoldenTest.
#
#   keyvalue.flightdata  (issue #2193) — the protobuf-encoded FlightData MESSAGE
#     SEQUENCE for the field failure shape (3-row cassandra_easy_stress.keyvalue:
#     key text, value text, 1 pk, 0 ck, with cqlite:pushdown), produced by the
#     SAME FlightDataEncoderBuilder path as production. Decoded by
#     FlightDataGoldenDecodeTest with arrow-java's Flight-level machinery.
#
# Usage: trino-connector/scripts/regen-arrow-golden.sh
# Run it whenever the server's Arrow schema/type mapping OR Flight encoding
# changes. Commit the regenerated blobs with `git add -f` (they live under a
# resources dir but are tracked fixtures).
set -euo pipefail

# Repo root = two levels up from this script (trino-connector/scripts).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GOLDEN_DIR="${REPO_ROOT}/trino-connector/src/test/resources/golden"
ARROWS_OUT="${GOLDEN_DIR}/all_scalars.arrows"
FLIGHTDATA_OUT="${GOLDEN_DIR}/keyvalue.flightdata"

echo "Regenerating goldens from the cqlite-flight server emission path..."
cargo run --quiet --manifest-path "${REPO_ROOT}/Cargo.toml" \
    -p cqlite-flight --example emit_arrow_golden -- "${ARROWS_OUT}" "${FLIGHTDATA_OUT}"

echo "Done. Review the diff and commit with:"
echo "  git add -f ${ARROWS_OUT} ${FLIGHTDATA_OUT}"
