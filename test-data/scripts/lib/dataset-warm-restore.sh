# shellcheck shell=bash
# dataset-warm-restore.sh — the warm-cache half of test-data/scripts/fetch-datasets.sh (#4508).
# Sourced by fetch-datasets.sh; it uses that script's DATASET_ROOT, ARCHIVE_DATASET_ROOT,
# TRACKED_GUARD_* state and restore_tracked_dataset_files.
#
# THE DEFECT. CI restores the cached dataset directory with `tar -xf` over the checkout, which
# overwrites same-named git-TRACKED files with the cache's (old) bytes and leaves every cached file
# behind. The pin file arrives inside the cache, so fetch-datasets.sh took the warm fast path and
# only repaired MISSING tracked files: a PR that changed tracked bytes was tested against the old
# bundle bytes, and a tracked file the PR deleted or renamed came back from the cache as an
# untracked twin that `git diff` cannot see (#4493: both the new and the old commitlog segment
# were present).
#
# THE CONTRACT. In CI, or with CQLITE_DATASETS_RESTORE_TRACKED=1, the warm path:
#   1. restores EVERY captured tracked file from the index (restore_tracked_dataset_files all, whose
#      post-condition is a clean `git diff`);
#   2. REMOVES, and logs, two classes of stale untracked file:
#        - a STALE TWIN: a file that was git-tracked when the cache was built and is no longer
#          tracked (the PR deleted or renamed it) but is still on disk;
#        - an EXTRA: a file in neither git nor the pinned bundle's manifest;
#   3. refuses to trust a cache that cannot be classified (built before #4508, so it has no
#      manifest): it restores the tracked files, then falls through to the cold path (rm -rf, extract
#      the pinned bundle, restore tracked), which is always classifiable.
# Locally, WITHOUT that opt-in, an unstaged edit to a tracked reference file is a developer's work in
# progress, so drift, twins and extras are only REPORTED.
#
# TWO LISTS ARE WRITTEN at cold extraction, next to `.dataset-pin` (all gitignored):
#   .dataset-manifest           the bundle's regular files, relative to the dataset root;
#   .dataset-tracked-at-build   the files git tracked at that moment.
# A bundle member that is NOT tracked-at-build (the .db binaries) is never touched. The twin class
# is derived from the cache's own build-time record, not from directories or extensions: the bundle
# ships its tracked-class files (jsonl, logs, TOC) in the same directories as its binaries.
# RESIDUAL (documented on #4508): a cache that is COLD on a PR branch records the PR's tracked set,
# so a file the PR deleted is a plain bundle member there and cannot be told from a binary; the
# canonical cache is built on main, so PR runs are warm.

# write_bundle_manifest <archive> — both build-time lists.
write_bundle_manifest() {
  tar -tzf "$1" | sed -n "s#^${ARCHIVE_DATASET_ROOT}/##p" | grep -v -e '/$' -e '^$' -e '/\._' -e '^\._' -e '\.DS_Store$' \
    | LC_ALL=C sort -u > "${DATASET_ROOT}/.dataset-manifest" || true
  if [ "${TRACKED_GUARD_STATE:-}" = "in-repo" ] && [ -s "${TRACKED_GUARD_LIST:-}" ]; then
    tr '\0' '\n' < "${TRACKED_GUARD_LIST}" | sed "s#^${TRACKED_GUARD_REL}/##" | LC_ALL=C sort -u \
      > "${DATASET_ROOT}/.dataset-tracked-at-build"
  else
    : > "${DATASET_ROOT}/.dataset-tracked-at-build"
  fi
}

# The cache KEY carries this format (the `fmt2` segment of `datasets-fmt2-<tag>-<sha>` in every workflow
# and in the restore action): caches built before the two lists existed can never match, so the first
# run cold-builds and SAVES one with the metadata instead of re-downloading on every job forever.
DATASET_CACHE_FORMAT=fmt2

_dataset_restore_mode() { [ -n "${CI:-}" ] || [ "${CQLITE_DATASETS_RESTORE_TRACKED:-}" = "1" ]; }

# warm_cache_usable — the warm fast path may be taken only if the cache can be classified.
warm_cache_usable() {
  _dataset_restore_mode || return 0
  # Undo the overlay first: the cold path refuses a tree with locally modified tracked files.
  restore_tracked_dataset_files all || exit 1
  if [ -s "${DATASET_ROOT}/.dataset-manifest" ] && [ -f "${DATASET_ROOT}/.dataset-tracked-at-build" ]; then
    return 0
  fi
  echo "NOTE: ${DATASET_ROOT} has no .dataset-manifest/.dataset-tracked-at-build (a cache from before #4508); its stale files cannot be classified, so forcing a COLD restore" >&2
  return 1
}

# _dataset_stale_lists — sets STALE_TWINS and STALE_EXTRAS (newline-separated, dataset-relative).
# Returns 0 classified, 2 not applicable (the dataset root is outside any git repo, so nothing is
# tracked), 1 a failure: an unclassifiable cache, an unprotected guard state or a temp-file error.
# An existing-but-EMPTY tracked list is the empty set, not a failure: a change that deletes every
# tracked dataset file must still have its cached twins removed.
_dataset_stale_lists() {
  local manifest="${DATASET_ROOT}/.dataset-manifest" built="${DATASET_ROOT}/.dataset-tracked-at-build"
  local now known disk
  STALE_TWINS=""; STALE_EXTRAS=""
  [ "${TRACKED_GUARD_STATE:-}" = "out-of-repo" ] && return 2
  [ -s "${manifest}" ] && [ -f "${built}" ] || return 1
  [ "${TRACKED_GUARD_STATE:-}" = "in-repo" ] && [ -f "${TRACKED_GUARD_LIST:-}" ] || return 1
  now="$(mktemp "${TMPDIR:-/tmp}/cqlite-ds-now.XXXXXX")" || return 1
  known="$(mktemp "${TMPDIR:-/tmp}/cqlite-ds-known.XXXXXX")" || { rm -f "${now}"; return 1; }
  disk="$(mktemp "${TMPDIR:-/tmp}/cqlite-ds-disk.XXXXXX")" || { rm -f "${now}" "${known}"; return 1; }
  tr '\0' '\n' < "${TRACKED_GUARD_LIST}" | sed "s#^${TRACKED_GUARD_REL}/##" | LC_ALL=C sort -u > "${now}"
  { cat "${now}" "${manifest}"; printf '%s\n' .dataset-pin .dataset-manifest .dataset-tracked-at-build; } | LC_ALL=C sort -u > "${known}"
  (cd "${DATASET_ROOT}" && find . -type f | sed 's#^\./##' | LC_ALL=C sort -u) > "${disk}"
  # twins: tracked at build, not tracked now, still on disk
  LC_ALL=C comm -23 "${built}" "${now}" > "${known}.twins"
  STALE_TWINS="$(LC_ALL=C comm -12 "${disk}" "${known}.twins")"
  # extras: on disk, in neither git nor the bundle manifest
  STALE_EXTRAS="$(LC_ALL=C comm -23 "${disk}" "${known}")"
  rm -f "${now}" "${known}" "${known}.twins" "${disk}"
}

# _dataset_remove_list <description> <newline-separated dataset-relative paths> — remove and log each.
_dataset_remove_list() {
  local what="$1" list="$2" rel
  [ -n "${list}" ] || return 0
  while IFS= read -r rel; do
    [ -n "${rel}" ] || continue
    rm -f "${DATASET_ROOT}/${rel}" && echo "REMOVED stale ${what}: ${rel}" >&2
  done <<<"${list}"
}

# warm_cache_reconcile — the warm fast path, after capture_tracked_dataset_files and warm_cache_usable
# (which, in restore mode, has already restored every tracked file from the index).
warm_cache_reconcile() {
  local drift rc
  if _dataset_restore_mode; then
    rc=0; _dataset_stale_lists || rc=$?
    if [ "${rc}" = 0 ]; then
      _dataset_remove_list "twin of a deleted/renamed tracked file" "${STALE_TWINS}"
      _dataset_remove_list "extra (in neither git nor the bundle)" "${STALE_EXTRAS}"
    elif [ "${rc}" != 2 ]; then
      echo "ERROR: could not classify stale files under ${DATASET_ROOT} (rc=${rc}); refusing to continue on contents that may be stale (#4508)" >&2
      exit 1
    fi
    return 0
  fi
  if [ "${TRACKED_GUARD_STATE:-}" = "in-repo" ] \
      && drift="$(guard_git -C "${TRACKED_GUARD_REPO}" diff --name-only -- ":(literal)${TRACKED_GUARD_REL}")" \
      && [ -n "${drift}" ]; then
    echo "WARNING: tracked file(s) under ${TRACKED_GUARD_REL} differ from the index (a cache overlay, or your edits); not touched without CQLITE_DATASETS_RESTORE_TRACKED=1:" >&2
    head -20 <<<"${drift}" | sed 's/^/WARNING:   /' >&2
  fi
  rc=0; _dataset_stale_lists || rc=$?
  if [ "${rc}" = 0 ]; then
    [ -z "${STALE_TWINS}" ] || { echo "WARNING: tracked-at-build file(s) no longer tracked but still on disk (CI removes them):" >&2; head -20 <<<"${STALE_TWINS}" | sed 's/^/WARNING:   /' >&2; }
    [ -z "${STALE_EXTRAS}" ] || { echo "WARNING: stale untracked file(s) in neither git nor the bundle manifest (CI removes them):" >&2; head -20 <<<"${STALE_EXTRAS}" | sed 's/^/WARNING:   /' >&2; }
  elif [ "${rc}" = 1 ]; then
    echo "NOTE: no .dataset-manifest/.dataset-tracked-at-build (a cache from before #4508); stale files cannot be classified" >&2
  fi
}
