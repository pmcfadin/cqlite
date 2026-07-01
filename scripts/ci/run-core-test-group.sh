#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
SKIP_TEST="test_legacy_format_allows_blob_fallback_with_feature"
SUMMARY_FILE="${GITHUB_STEP_SUMMARY:-/dev/null}"

if [ -z "${MODE}" ]; then
  echo "usage: $0 <lib-doc>" >&2
  exit 2
fi

write_summary_header() {
  local title="$1"
  {
    echo "### ${title}"
    echo
    echo "| Group | Status | Duration |"
    echo "|---|---:|---:|"
  } >> "${SUMMARY_FILE}"
}

write_summary_row() {
  local label="$1"
  local status="$2"
  local duration="$3"
  printf '| %s | %s | %ss |\n' "${label}" "${status}" "${duration}" >> "${SUMMARY_FILE}"
}

timed_run() {
  local label="$1"
  shift

  local start end duration status
  start="$(date +%s)"
  echo "::group::${label}"
  printf '+'
  printf ' %q' "$@"
  printf '\n'

  set +e
  "$@"
  status="$?"
  set -e

  end="$(date +%s)"
  duration="$((end - start))"

  if [ "${status}" -eq 0 ]; then
    write_summary_row "${label}" "success" "${duration}"
  else
    write_summary_row "${label}" "failed (${status})" "${duration}"
  fi

  echo "::endgroup::"
  return "${status}"
}

run_lib_doc() {
  write_summary_header "Core lib/doc test timings"

  if [ "${CORE_TEST_DRY_RUN:-}" = "1" ]; then
    echo "Dry run: would run cqlite-core lib tests, doc tests, and example builds"
    write_summary_row "cqlite-core lib/doc dry run" "success" "0"
    return 0
  fi

  timed_run "cqlite-core lib tests" \
    cargo test --package cqlite-core --features cli-helpers --lib -- --skip "${SKIP_TEST}"
  timed_run "cqlite-core doc tests" \
    cargo test --package cqlite-core --features cli-helpers --doc
  timed_run "cqlite-core example builds" \
    cargo test --package cqlite-core --features cli-helpers --examples --no-run
}

case "${MODE}" in
  lib-doc)
    run_lib_doc
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    exit 2
    ;;
esac
