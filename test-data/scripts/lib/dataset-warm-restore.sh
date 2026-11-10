# shellcheck shell=bash
# dataset-warm-restore.sh — the warm-cache half of test-data/scripts/fetch-datasets.sh (#4508).
# Sourced by fetch-datasets.sh; it uses that script's DATASET_ROOT, ARCHIVE_DATASET_ROOT,
# TRACKED_GUARD_* state and restore_tracked_dataset_files.
#
# THE DEFECT. CI restores the cached dataset directory with `tar -xf` over the checkout, which
# overwrites same-named git-TRACKED files with the cache's (old) bytes and leaves cached extras
# behind. The pin file arrives inside the cache, so fetch-datasets.sh took the warm fast path and
# only repaired MISSING tracked files: a PR that changed tracked bytes under test-data/datasets was
# tested against the old bundle bytes and still passed (#4493's renamed commitlog segment only
# failed because it produced a duplicate instead of an overwrite).
#
# THE CONTRACT. After ANY restore path the tracked files under the dataset root are byte-identical
# to the index:
#   * CI (or CQLITE_DATASETS_RESTORE_TRACKED=1): the warm path restores EVERY captured tracked file
#     from the index (`restore_tracked_dataset_files all`, whose post-condition is a clean `git diff`
#     and which returns non-zero otherwise).
#   * locally, WITHOUT that opt-in, an unstaged edit to a tracked reference file is a developer's
#     work in progress (the `missing-only` pre-flight exists to spare it), so it is only REPORTED.
#   * untracked files that are in neither git nor the pinned bundle's manifest are reported.
# The manifest is `.dataset-manifest`, the bundle's file list, written next to `.dataset-pin` when the
# bundle is extracted. A cache built before #4508 has none; extras are then not classified.

# write_bundle_manifest <archive> — the regular files of the bundle, relative to the dataset root.
write_bundle_manifest() {
  tar -tzf "$1" | sed -n "s#^${ARCHIVE_DATASET_ROOT}/##p" | grep -v -e '/$' -e '^$' -e '/\._' -e '^\._' -e '\.DS_Store$' \
    | LC_ALL=C sort -u > "${DATASET_ROOT}/.dataset-manifest" || true
}

# report_dataset_extras — untracked files that neither git nor the bundle manifest accounts for.
report_dataset_extras() {
  local manifest="${DATASET_ROOT}/.dataset-manifest" known disk extras count
  if [ ! -s "${manifest}" ]; then
    echo "NOTE: ${manifest} is absent (a cache from before #4508); stale untracked extras cannot be classified" >&2
    return 0
  fi
  [ "${TRACKED_GUARD_STATE:-}" = "in-repo" ] && [ -s "${TRACKED_GUARD_LIST:-}" ] || return 0
  known="$(mktemp "${TMPDIR:-/tmp}/cqlite-ds-known.XXXXXX")" || return 0
  disk="$(mktemp "${TMPDIR:-/tmp}/cqlite-ds-disk.XXXXXX")" || { rm -f "${known}"; return 0; }
  { tr '\0' '\n' < "${TRACKED_GUARD_LIST}" | sed "s#^${TRACKED_GUARD_REL}/##"
    cat "${manifest}"; printf '%s\n' .dataset-pin .dataset-manifest; } | LC_ALL=C sort -u > "${known}"
  (cd "${DATASET_ROOT}" && find . -type f | sed 's#^\./##' | LC_ALL=C sort -u) > "${disk}"
  extras="$(LC_ALL=C comm -23 "${disk}" "${known}")"
  rm -f "${known}" "${disk}"
  [ -n "${extras}" ] || return 0
  count="$(wc -l <<<"${extras}" | tr -d ' ')"
  echo "WARNING: ${count} stale untracked file(s) under ${DATASET_ROOT} are in neither git nor the pinned bundle manifest:" >&2
  head -20 <<<"${extras}" | sed 's/^/WARNING:   /' >&2
}

# warm_cache_reconcile — called on the warm fast path, after capture_tracked_dataset_files.
warm_cache_reconcile() {
  local drift
  if [ -n "${CI:-}" ] || [ "${CQLITE_DATASETS_RESTORE_TRACKED:-}" = "1" ]; then
    restore_tracked_dataset_files all || exit 1
  elif [ "${TRACKED_GUARD_STATE:-}" = "in-repo" ] \
      && drift="$(guard_git -C "${TRACKED_GUARD_REPO}" diff --name-only -- ":(literal)${TRACKED_GUARD_REL}")" \
      && [ -n "${drift}" ]; then
    echo "WARNING: tracked file(s) under ${TRACKED_GUARD_REL} differ from the index (a cache overlay, or your edits); not touched without CQLITE_DATASETS_RESTORE_TRACKED=1:" >&2
    head -20 <<<"${drift}" | sed 's/^/WARNING:   /' >&2
  fi
  report_dataset_extras
}
