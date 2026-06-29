#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
SKIP_TEST="test_legacy_format_allows_blob_fallback_with_feature"
SUMMARY_FILE="${GITHUB_STEP_SUMMARY:-/dev/null}"

if [ -z "${MODE}" ]; then
  echo "usage: $0 <lib-doc|integration-shard>" >&2
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

run_integration_shard() {
  local shard_index="${CORE_TEST_SHARD_INDEX:-}"
  local shard_total="${CORE_TEST_SHARD_TOTAL:-}"

  if [ -z "${shard_index}" ] || [ -z "${shard_total}" ]; then
    echo "CORE_TEST_SHARD_INDEX and CORE_TEST_SHARD_TOTAL must be set" >&2
    exit 2
  fi

  if ! [[ "${shard_index}" =~ ^[0-9]+$ ]] || ! [[ "${shard_total}" =~ ^[0-9]+$ ]]; then
    echo "Shard index and total must be non-negative integers" >&2
    exit 2
  fi

  if [ "${shard_total}" -le 0 ] || [ "${shard_index}" -ge "${shard_total}" ]; then
    echo "Invalid shard ${shard_index}/${shard_total}" >&2
    exit 2
  fi

  local all_tests=()
  local selected_tests=()
  mapfile -t all_tests < <(
    find cqlite-core/tests -maxdepth 1 -type f -name '*.rs' -print \
      | sed 's#^cqlite-core/tests/##; s#\.rs$##' \
      | sort
  )

  local idx
  for idx in "${!all_tests[@]}"; do
    if [ "$((idx % shard_total))" -eq "${shard_index}" ]; then
      selected_tests+=("${all_tests[$idx]}")
    fi
  done

  if [ "${#selected_tests[@]}" -eq 0 ]; then
    echo "No tests selected for shard ${shard_index}/${shard_total}" >&2
    exit 1
  fi

  {
    echo "### Core integration shard ${shard_index}/${shard_total}"
    echo
    echo "Selected ${#selected_tests[@]} of ${#all_tests[@]} cqlite-core integration test binaries."
    echo
    echo "<details><summary>Selected test binaries</summary>"
    echo
    printf -- '- `%s`\n' "${selected_tests[@]}"
    echo
    echo "</details>"
    echo
    echo "| Group | Status | Duration |"
    echo "|---|---:|---:|"
  } >> "${SUMMARY_FILE}"

  if [ "${CORE_TEST_DRY_RUN:-}" = "1" ]; then
    echo "Dry run: selected ${#selected_tests[@]} of ${#all_tests[@]} cqlite-core integration test binaries"
    printf '%s\n' "${selected_tests[@]}"
    write_summary_row "cqlite-core integration shard ${shard_index}/${shard_total} dry run" "success" "0"
    return 0
  fi

  local cmd=(cargo test --package cqlite-core --features cli-helpers)
  local test_name
  for test_name in "${selected_tests[@]}"; do
    cmd+=(--test "${test_name}")
  done
  cmd+=(-- --skip "${SKIP_TEST}")

  timed_run "cqlite-core integration shard ${shard_index}/${shard_total}" "${cmd[@]}"
}

case "${MODE}" in
  lib-doc)
    run_lib_doc
    ;;
  integration-shard)
    run_integration_shard
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    exit 2
    ;;
esac
