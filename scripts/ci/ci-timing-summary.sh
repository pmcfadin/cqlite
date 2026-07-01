#!/usr/bin/env bash
set -euo pipefail

TIMING_FILE="${CI_TIMING_FILE:-${RUNNER_TEMP:-/tmp}/ci-timing.tsv}"

usage() {
  cat >&2 <<'USAGE'
Usage:
  ci-timing-summary.sh measure <label> -- <command> [args...]
  ci-timing-summary.sh note <label> <status> <duration-seconds>
  ci-timing-summary.sh summary [title]

Rows are written to $CI_TIMING_FILE, or $RUNNER_TEMP/ci-timing.tsv by default.
USAGE
}

ensure_timing_dir() {
  mkdir -p "$(dirname "${TIMING_FILE}")"
}

format_seconds() {
  local total="${1:-0}"
  local minutes seconds
  if ! [[ "${total}" =~ ^[0-9]+$ ]]; then
    printf '%s' "${total}"
    return
  fi
  minutes=$((total / 60))
  seconds=$((total % 60))
  printf '%dm %02ds' "${minutes}" "${seconds}"
}

md_escape() {
  local value="${1:-}"
  value="${value//$'\t'/ }"
  value="${value//|/\\|}"
  printf '%s' "${value}"
}

append_row() {
  local label="$1"
  local status="$2"
  local duration="$3"
  local started="$4"
  local finished="$5"

  ensure_timing_dir
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "${label}" \
    "${status}" \
    "${duration}" \
    "${started}" \
    "${finished}" >> "${TIMING_FILE}"
}

measure_command() {
  if [ "$#" -lt 3 ]; then
    usage
    exit 2
  fi

  local label="$1"
  shift
  if [ "${1:-}" != "--" ]; then
    usage
    exit 2
  fi
  shift

  local started_epoch finished_epoch duration started_at finished_at status status_text
  started_epoch="$(date +%s)"
  started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  set +e
  "$@"
  status="$?"
  set -e

  finished_epoch="$(date +%s)"
  finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  duration="$((finished_epoch - started_epoch))"

  if [ "${status}" -eq 0 ]; then
    status_text="success"
  else
    status_text="failed (${status})"
  fi

  append_row "${label}" "${status_text}" "${duration}" "${started_at}" "${finished_at}"
  exit "${status}"
}

note_timing() {
  if [ "$#" -ne 3 ]; then
    usage
    exit 2
  fi

  local label="$1"
  local status="$2"
  local duration="$3"
  local now
  now="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  if [[ "${status}" =~ ^[0-9]+$ ]]; then
    if [ "${status}" -eq 0 ]; then
      status="success"
    else
      status="failed (${status})"
    fi
  fi

  append_row "${label}" "${status}" "${duration}" "recorded" "${now}"
}

write_summary() {
  local title="${1:-CI runtime timings}"
  local summary_file="${GITHUB_STEP_SUMMARY:-}"

  emit_summary() {
    local label status duration started finished
  
    echo "### ${title}"
    echo
    if [ ! -s "${TIMING_FILE}" ]; then
      echo "No timing rows were recorded."
      echo
      return
    fi
  
    echo "| Step | Status | Duration | Started | Finished |"
    echo "| --- | --- | ---: | --- | --- |"
  
    while IFS=$'\t' read -r label status duration started finished; do
      printf '| %s | %s | %s | %s | %s |\n' \
        "$(md_escape "${label}")" \
        "$(md_escape "${status}")" \
        "$(format_seconds "${duration}")" \
        "$(md_escape "${started}")" \
        "$(md_escape "${finished}")"
    done < "${TIMING_FILE}"
    echo
  }
  
  if [ -n "${summary_file}" ]; then
    emit_summary >> "${summary_file}"
  else
    emit_summary
  fi
}

main() {
  local command="${1:-}"
  shift || true

  case "${command}" in
    measure)
      measure_command "$@"
      ;;
    note)
      note_timing "$@"
      ;;
    summary)
      write_summary "$@"
      ;;
    *)
      usage
      exit 2
      ;;
  esac
}

main "$@"
