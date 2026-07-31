#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v3 DATASET_ASSET=cassandra5-small-full-v3.5.tar.gz DATASET_SHA256=414195074f6df446a7381aad051af84158e9a021a6e2cd21cbc6c3ad0be1ba16 ./test-data/scripts/fetch-datasets.sh
#        ./test-data/scripts/fetch-datasets.sh --verify-only   # report usability only; mutates nothing

# ---- STRICT argument validation, FIRST, before ANY filesystem work -------------
# This script's default path is DESTRUCTIVE (`rm -rf "${DATASET_ROOT}"` before
# extraction). Until #3131 it accepted no flags, so an unrecognized argument was
# harmless. Adding `--verify-only` created a data-loss hazard: matching the flag only
# as `$1` (or ignoring extra args) means `--quiet --verify-only`, `-verify-only`, or any
# typo SILENTLY selects the destructive path and rm -rf's the operator's corpus while
# they believe they asked for a read-only probe. Introducing a flag obliges fail-closed
# rejection of EVERY unrecognized argument, so parsing happens here — before the pin
# load, before root canonicalization (which used to `mkdir -p` the parent), before
# anything can touch the filesystem.
VERIFY_ONLY=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --verify-only) VERIFY_ONLY=1; shift ;;
    -h|--help)
      echo "usage: fetch-datasets.sh [--verify-only]"
      echo "  --verify-only  report whether \$CQLITE_DATASETS_ROOT is usable and print the"
      echo "                 guaranteed export line; downloads/extracts/removes/creates nothing."
      exit 0 ;;
    *)
      echo "ERROR: unrecognized argument '$1'" >&2
      echo "ERROR: refusing to continue — this script's default path is DESTRUCTIVE" >&2
      echo "ERROR: (rm -rf \"\${DATASET_ROOT}\" before extraction), so an unrecognized" >&2
      echo "ERROR: argument must never be silently ignored." >&2
      echo "ERROR: usage: fetch-datasets.sh [--verify-only]" >&2
      exit 2 ;;
  esac
done

# The canonical asset/tag/sha live in ONE tracked file (issue #2646):
# test-data/dataset-pin.env. Load it for the defaults so this helper never
# drifts from the workflows. Precedence: explicit DATASET_* env (CI passes the
# workflow-declared pin) > tracked pin file > historical hardcoded fallback
# (older checkouts without the pin file). We capture the incoming env FIRST so
# sourcing the pin file cannot clobber a real override.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIN_ENV="${SCRIPT_DIR}/../dataset-pin.env"
_ENV_TAG="${DATASET_TAG:-}"
_ENV_ASSET="${DATASET_ASSET:-}"
_ENV_SHA="${DATASET_SHA256:-}"
_PIN_TAG=""; _PIN_ASSET=""; _PIN_SHA=""
if [ -f "${PIN_ENV}" ]; then
  # shellcheck disable=SC1090
  . "${PIN_ENV}"
  _PIN_TAG="${DATASET_TAG:-}"
  _PIN_ASSET="${DATASET_ASSET:-}"
  _PIN_SHA="${DATASET_SHA256:-}"
fi

TAG="${_ENV_TAG:-${_PIN_TAG:-datasets-v3}}"
ASSET="${_ENV_ASSET:-${_PIN_ASSET:-cassandra5-small-full-v3.5.tar.gz}}"
SHA256_EXPECTED="${_ENV_SHA:-${_PIN_SHA:-414195074f6df446a7381aad051af84158e9a021a6e2cd21cbc6c3ad0be1ba16}}"
DATASET_ROOT="${CQLITE_DATASETS_ROOT:-test-data/datasets}"
ARCHIVE_DATASET_ROOT="test-data/datasets"
PIN_FILE="${DATASET_ROOT}/.dataset-pin"
ASSET_PATH="/tmp/${ASSET}"
WIDE_PARTITION_DIR="${DATASET_ROOT}/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
WIDE_PARTITION_GOLDEN="${WIDE_PARTITION_DIR}/nb-2-big-Data.db.jsonl"
# Exact promoted wide_partition reference binaries the byte_for_byte parity
# scenarios + the digest strict test require (mirrors REQUIRED_BINARY_COMPONENTS
# in cqlite-core/tests/issue_993_wide_partition_promoted_index_parity.rs). A
# dataset missing any of these must force a re-fetch (issue #1185 fail-closed).
WIDE_PARTITION_REQUIRED_COMPONENTS=(
  "nb-2-big-Data.db"
  "nb-2-big-Index.db"
  "nb-2-big-Digest.crc32"
  "nb-2-big-CompressionInfo.db"
)

if [ -z "${DATASET_ROOT}" ] || [ "${DATASET_ROOT}" = "/" ]; then
  echo "ERROR: unsafe CQLITE_DATASETS_ROOT='${DATASET_ROOT}'" >&2
  exit 1
fi

fail_unsafe_dataset_root() {
  echo "ERROR: unsafe CQLITE_DATASETS_ROOT='${DATASET_ROOT}': $1" >&2
  exit 1
}

canonicalize_dataset_root() {
  local raw_root="$1"
  local abs_root parent base parent_abs repo_root

  case "${raw_root}" in
    ""|"/"|".")
      fail_unsafe_dataset_root "must point at a dataset directory, not a filesystem root"
      ;;
    /*) abs_root="${raw_root}" ;;
    *) abs_root="${PWD}/${raw_root}" ;;
  esac

  parent="$(dirname "${abs_root}")"
  base="$(basename "${abs_root}")"

  [ "${base}" = "datasets" ] \
    || fail_unsafe_dataset_root "final path component must be 'datasets'"

  # --verify-only promises to mutate NOTHING (#3131 blocker B2): this `mkdir -p` runs
  # BEFORE the mode dispatch, so an unqualified call created the parent of a root it was
  # about to report unusable (e.g. probing /mnt/corpus/v4/datasets on a box where
  # /mnt/corpus is empty created /mnt/corpus/v4). Under --verify-only we therefore never
  # create anything: a parent that does not exist simply means the root is unusable, and
  # saying so is the probe's whole job.
  if [ "${VERIFY_ONLY}" = 1 ]; then
    [ -d "${parent}" ] \
      || fail_unsafe_dataset_root "parent directory ${parent} does not exist — root unusable (nothing created: --verify-only)"
  else
    mkdir -p "${parent}"
  fi
  parent_abs="$(cd "${parent}" && pwd -P)"
  [ "${parent_abs}" != "/" ] \
    || fail_unsafe_dataset_root "refusing to replace a top-level /datasets directory"

  abs_root="${parent_abs}/${base}"

  if repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
    repo_root="$(cd "${repo_root}" && pwd -P)"
    [ "${abs_root}" != "${repo_root}" ] \
      || fail_unsafe_dataset_root "refusing to replace the repository root"
  fi

  [ "${abs_root}" != "${HOME:-}" ] \
    || fail_unsafe_dataset_root "refusing to replace HOME"
  [ "${abs_root}" != "${TMPDIR:-}" ] \
    || fail_unsafe_dataset_root "refusing to replace TMPDIR"
  [ "${abs_root}" != "/tmp" ] \
    || fail_unsafe_dataset_root "refusing to replace /tmp"
  [ "${abs_root}" != "/private/tmp" ] \
    || fail_unsafe_dataset_root "refusing to replace /private/tmp"

  printf '%s\n' "${abs_root}"
}

DATASET_ROOT="$(canonicalize_dataset_root "${DATASET_ROOT}")"
export CQLITE_DATASETS_ROOT="${DATASET_ROOT}"
PIN_FILE="${DATASET_ROOT}/.dataset-pin"
WIDE_PARTITION_DIR="${DATASET_ROOT}/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
WIDE_PARTITION_GOLDEN="${WIDE_PARTITION_DIR}/nb-2-big-Data.db.jsonl"

write_pin() {
  mkdir -p "${DATASET_ROOT}"
  {
    echo "tag=${TAG}"
    echo "asset=${ASSET}"
    echo "sha256=${SHA256_EXPECTED}"
  } > "${PIN_FILE}"
}

restore_ci_tracked_dataset_files() {
  [ -n "${CI:-}" ] || return 0
  command -v git >/dev/null 2>&1 || return 0
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || return 0

  local repo_root dataset_abs dataset_rel tracked_files
  repo_root="$(git rev-parse --show-toplevel)"
  case "${DATASET_ROOT}" in
    /*) dataset_abs="${DATASET_ROOT}" ;;
    *) dataset_abs="${PWD}/${DATASET_ROOT}" ;;
  esac

  case "${dataset_abs}" in
    "${repo_root}"/*) dataset_rel="${dataset_abs#"${repo_root}/"}" ;;
    *) return 0 ;;
  esac

  tracked_files="$(git -C "${repo_root}" ls-files -- "${dataset_rel}")"
  if [ -n "${tracked_files}" ]; then
    echo "Restoring git-tracked dataset reference files under ${dataset_rel}"
    git -C "${repo_root}" restore --source=HEAD -- "${dataset_rel}" 2>/dev/null || true
  fi
}

# Validates the dataset CONTENT only (no pin tag/asset/sha checks). A freshly
# extracted archive is in exactly this state — content present but the pin not
# yet stamped — so the post-extraction validation uses this to confirm the
# archive is complete (incl. the exact wide_partition reference binaries) before
# write_pin runs.
has_required_content() {
  [ -f "${DATASET_ROOT}/metadata.yml" ] || return 1
  [ -s "${WIDE_PARTITION_GOLDEN}" ] || return 1

  local core_fixture
  core_fixture="$(find "${DATASET_ROOT}/sstables/test_basic" -path '*simple_table-*-Data.db' -print -quit 2>/dev/null || true)"
  [ -n "${core_fixture}" ] || return 1

  local data_count index_count summary_count statistics_count
  data_count="$(find "${DATASET_ROOT}" -name '*-Data.db' 2>/dev/null | wc -l | tr -d ' ')"
  index_count="$(find "${DATASET_ROOT}" -name '*-Index.db' 2>/dev/null | wc -l | tr -d ' ')"
  summary_count="$(find "${DATASET_ROOT}" -name '*-Summary.db' 2>/dev/null | wc -l | tr -d ' ')"
  statistics_count="$(find "${DATASET_ROOT}" -name '*-Statistics.db' 2>/dev/null | wc -l | tr -d ' ')"

  [ "${data_count}" -gt 0 ] || return 1
  [ "${index_count}" -gt 0 ] || return 1
  [ "${summary_count}" -gt 0 ] || return 1
  [ "${statistics_count}" -gt 0 ] || return 1

  # The global counts above can be satisfied by other tables; explicitly require
  # the EXACT promoted wide_partition reference binaries so a partial/stale
  # dataset (other tables present, wide_partition absent) cannot be accepted and
  # stamped as the new pin (issue #1185 fail-closed).
  local component
  for component in "${WIDE_PARTITION_REQUIRED_COMPONENTS[@]}"; do
    [ -f "${WIDE_PARTITION_DIR}/${component}" ] || return 1
  done
}

# Strict fast-path check: content present AND pinned to the expected v3.5 tag/
# asset/sha. The skip-download path requires BOTH so a stale/unpinned dataset
# forces a re-fetch (issue #1185 fail-closed).
has_required_dataset() {
  has_required_content || return 1

  # The pin file MUST exist and match. A missing pin is NOT acceptable — it would
  # otherwise let an unverified dataset fall through and be re-stamped (#1185).
  [ -f "${PIN_FILE}" ] || return 1
  grep -qx "tag=${TAG}" "${PIN_FILE}" || return 1
  grep -qx "asset=${ASSET}" "${PIN_FILE}" || return 1
  grep -qx "sha256=${SHA256_EXPECTED}" "${PIN_FILE}" || return 1
}

# guarantee_usable_root (issue #3131 item 2): a ZERO EXIT MUST MEAN "this root is
# usable" — on the warm-cache path exactly as much as after a fresh extraction.
#
# Before #3131 the warm path's sole output was
#   Dataset <asset> (tag <tag>) already present in <root>; skipping download
# and exit 0. Nothing in that told an operator WHICH root was guaranteed, so a green
# fetch was not evidence that any particular tree gained fixtures — and the
# CLAUDE.md-documented `CQLITE_DATASETS_ROOT=$PWD/test-data/datasets` could still be
# corpus-less, because an already-exported CQLITE_DATASETS_ROOT sends the extraction
# somewhere else entirely. That is how the documented remedy silently failed to remedy.
#
# So: re-verify the CONTENT at the extraction target (independently of the
# .dataset-pin fast path that got us here — a pin file is a claim, not the corpus),
# fail loudly with a remedy when it is not there, and print the EXACT
# `export CQLITE_DATASETS_ROOT=<absolute path>` line this run guarantees.
#
# Deliberately does NOT touch the `rm -rf "${DATASET_ROOT}"` / the
# `[ -n "${CI:-}" ] || return 0` short-circuit in restore_ci_tracked_dataset_files:
# that sibling defect is issue #2878, a separate delivery. Not widened here.
guarantee_usable_root() {
  local phase="$1"

  if ! has_required_content; then
    echo "ERROR: ${DATASET_ROOT} does not hold a usable dataset corpus (${phase})" >&2
    echo "ERROR: required content missing — expected at least metadata.yml, a" >&2
    echo "ERROR: test_basic/simple_table-*-Data.db, and the promoted wide_partition" >&2
    echo "ERROR: reference binaries under" >&2
    echo "ERROR:   ${WIDE_PARTITION_DIR}" >&2
    echo "ERROR: remedy: re-run this script with the pin cleared so it re-downloads:" >&2
    echo "ERROR:   rm -f ${PIN_FILE} && CQLITE_DATASETS_ROOT=${DATASET_ROOT} bash test-data/scripts/fetch-datasets.sh" >&2
    exit 1
  fi

  local data_count
  data_count="$(find "${DATASET_ROOT}" -name '*-Data.db' 2>/dev/null | wc -l | tr -d ' ')"

  echo "Dataset root VERIFIED (${phase}): ${DATASET_ROOT} — ${data_count} *-Data.db present"
  echo "Use EXACTLY this root (the only one this run guarantees):"
  echo
  # %q, not plain interpolation (roborev job 8, finding 3): the promise is an EXACT
  # copy-pasteable line, and a root containing a space or a shell metacharacter would
  # otherwise print a command that breaks (or does something else) when pasted. For a
  # metacharacter-free path %q is a no-op, so the common case is byte-identical.
  # shellcheck disable=SC2059  # %q is the point; the value is the argument, not the format
  printf '  export CQLITE_DATASETS_ROOT=%q\n' "${DATASET_ROOT}"
  echo

  # If a pre-existing CQLITE_DATASETS_ROOT sent the corpus somewhere other than the
  # checkout's test-data/datasets, say so: the documented default would otherwise look
  # like a valid choice and yield a corpus-less root (the #3131 report).
  local repo_root repo_default
  if repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
    repo_default="$(cd "${repo_root}" && pwd -P)/test-data/datasets"
    if [ "${DATASET_ROOT}" != "${repo_default}" ]; then
      echo "NOTE: this run populated ${DATASET_ROOT}, NOT the checkout default"
      echo "NOTE:   ${repo_default}"
      echo "NOTE: (CQLITE_DATASETS_ROOT was already set in the environment). Exporting the"
      echo "NOTE: checkout default instead would give you a corpus-less root — use the"
      echo "NOTE: export line above."
    fi
  fi
  # The CQL schema fixtures are COMMITTED SOURCE resolved checkout-relative (#3148);
  # they are NOT part of this archive and need no environment variable. Said here
  # because the pre-#3148 helpers looked for them at <root>/../schemas, so operators
  # learned to expect a sibling this script never creates.
  echo "NOTE: CQL schema fixtures (test-data/schemas) are committed source, resolved"
  echo "NOTE: checkout-relative — not fetched here and not a sibling of this root (#3148)."
}

# --verify-only (issue #3131): report whether the resolved root is usable and print the
# guaranteed export line, WITHOUT downloading, extracting, removing or re-pinning
# anything. Two reasons this exists rather than being a pure internal:
#   1. It makes guarantee_usable_root's FAILURE path directly exercisable. On the
#      warm-cache path the pin fast-path implies the content check, so a self-test could
#      otherwise only ever observe it passing — and a check observed only passing is not
#      a check (the same one-sided-verification mistake as #3148).
#   2. It gives an operator (or a preflight) a cheap "is this root usable?" probe that
#      cannot mutate the tree.
# The flag itself is parsed (and every unrecognized argument rejected) at the TOP of this
# script, before any filesystem work; canonicalize_dataset_root honors VERIFY_ONLY by
# never creating the parent directory.
if [ "${VERIFY_ONLY}" = 1 ]; then
  guarantee_usable_root "verify-only (no download, no extraction)"
  exit 0
fi

restore_ci_tracked_dataset_files

if has_required_dataset; then
  write_pin
  echo "Dataset ${ASSET} (tag ${TAG}) already present in ${DATASET_ROOT}; skipping download"
  guarantee_usable_root "warm cache, download skipped"
  exit 0
fi

echo "Fetching dataset ${ASSET} (tag ${TAG})"
mkdir -p "${DATASET_ROOT}"
curl -fsSL -o "${ASSET_PATH}" "https://github.com/pmcfadin/cqlite/releases/download/${TAG}/${ASSET}"

if command -v sha256sum >/dev/null 2>&1; then
  echo "${SHA256_EXPECTED}  ${ASSET_PATH}" | sha256sum -c -
elif command -v shasum >/dev/null 2>&1; then
  ACTUAL="$(shasum -a 256 "${ASSET_PATH}" | awk '{print $1}')"
  test "${ACTUAL}" = "${SHA256_EXPECTED}" || { echo "SHA256 mismatch"; exit 1; }
else
  # Fail closed in CI (issue #1024): a missing checksum tool must NOT let an
  # unverified asset through where parity is gated. When CQLITE_PARITY_REQUIRE_DATASETS
  # (exported by the parity workflow) or the generic CI signal is set, an
  # unverifiable dataset is a gate failure — there is no safe way to admit the
  # asset without confirming its provenance against the pinned SHA256.
  #
  # On a local/dev machine that simply lacks both tools, fall through with a
  # loud warning so iteration is not blocked; the operator can opt back into
  # fail-closed behavior with CQLITE_PARITY_REQUIRE_DATASETS=1.
  if [ "${CQLITE_PARITY_REQUIRE_DATASETS:-}" = "1" ] || [ -n "${CI:-}" ]; then
    echo "ERROR: no sha256 checker found (need sha256sum or shasum) — cannot verify dataset provenance" >&2
    exit 1
  else
    echo "WARNING: no sha256 tool; skipping checksum verification (set CQLITE_PARITY_REQUIRE_DATASETS=1 to enforce)" >&2
  fi
fi

rm -rf "${DATASET_ROOT}"
if [ "${DATASET_ROOT}" = "${ARCHIVE_DATASET_ROOT}" ]; then
  tar -xzf "${ASSET_PATH}" -C . --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'
else
  EXTRACT_TMP="$(mktemp -d)"
  trap 'rm -rf "${EXTRACT_TMP:-}"' EXIT
  tar -xzf "${ASSET_PATH}" -C "${EXTRACT_TMP}" --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'

  if [ ! -d "${EXTRACT_TMP}/${ARCHIVE_DATASET_ROOT}" ]; then
    echo "ERROR: dataset archive did not contain ${ARCHIVE_DATASET_ROOT}" >&2
    exit 1
  fi

  mkdir -p "$(dirname "${DATASET_ROOT}")"
  mv "${EXTRACT_TMP}/${ARCHIVE_DATASET_ROOT}" "${DATASET_ROOT}"
fi
restore_ci_tracked_dataset_files

# Remove macOS AppleDouble shadow files (`._*`). The archive may contain them
# when produced on macOS, and they break test helpers that scan for files by
# suffix (e.g., `*-Data.db` matches both the real file and `._..-Data.db`).
find "${DATASET_ROOT}" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

# Validate the freshly extracted CONTENT (not the pin — package_datasets.sh does
# not embed a .dataset-pin in the archive). A bad/partial archive still fails
# loudly here; the pin is stamped only after content is confirmed.
if ! has_required_content; then
  echo "ERROR: dataset extraction did not produce required Cassandra SSTable components in ${DATASET_ROOT}" >&2
  exit 1
fi

write_pin
echo "Dataset extracted to ${DATASET_ROOT}"
guarantee_usable_root "fresh extraction"
