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
# THE CONTRACT. In CI, or with CQLITE_DATASETS_RESTORE_TRACKED=1, the fetch (warm OR cold):
#   1. restores EVERY captured tracked file from the index BEFORE any presence/pin decision (so a
#      restore-key hit from an older pin cannot leave overlaid tracked bytes for the cold path to
#      reject as "local modifications");
#   2. REMOVES, and logs, two classes of stale untracked file:
#        - a STALE TWIN: a path in test-data/dataset-bundle-tracked.txt (the bundle's own tracked-class
#          files, pinned to the bundle's sha256) that git no longer tracks (the PR deleted or renamed
#          it) but that is still on disk;
#        - an EXTRA: a file in neither git nor the bundle's file list (.dataset-manifest);
#   3. refuses to trust a cache that cannot be classified (no .dataset-manifest: a cache from before
#      #4508): it falls through to the cold path (rm -rf, extract the pinned bundle, restore tracked).
# Locally, WITHOUT that opt-in, an unstaged edit to a tracked reference file is a developer's work in
# progress, so drift, twins and extras are only REPORTED.
#
# WHY THE TWIN LIST IS COMMITTED, NOT CACHE-DERIVED. An earlier version recorded `git ls-files` at
# cache-build time. A cache built COLD on a PR branch then recorded the PR's tracked set, so a path the
# PR had deleted or renamed looked like a legitimate bundle member forever. The bundle's tracked set is
# a property of the BUNDLE, not of the cache or the checkout: test-data/dataset-bundle-tracked.txt is
# generated from the pinned bundle itself (test-data/scripts/gen-dataset-tracked-manifest.sh: bundle
# members that are tracked), carries `# sha256=<pin>`, and is verified against the pin by
# scripts/ci/check-dataset-pin.sh. It is the same on every run, cold or warm, on main or a PR.
#
# PATH SAFETY. Every list is NUL-separated on disk and a tracked/on-disk path containing a newline is
# refused outright (the comparison tools are line-based). A candidate for removal must be a clean
# relative path (no `..` component, no absolute prefix), not a symlink, and its parent must RESOLVE
# under the physical dataset root; otherwise it is refused and the run fails.

# git_posix_path <path> — Git for Windows prints the paths it reports (rev-parse --show-toplevel,
# --absolute-git-dir, --git-common-dir) in WINDOWS form (D:/a/x), which a POSIX-minded caller reads as
# RELATIVE and joins onto $PWD. Under Git Bash the same file is /d/a/x; cygpath converts. Anything that is
# not Windows-form (every Linux/macOS path) is returned unchanged. Paths git prints REPO-RELATIVE
# (ls-files, diff --name-only) need no conversion.
git_posix_path() {
  case "$1" in
    [A-Za-z]:[\\/]*) if command -v cygpath >/dev/null 2>&1; then cygpath -u "$1"; return; fi ;;
  esac
  printf '%s\n' "$1"
}

# write_bundle_manifest <archive> — the bundle's regular files (the cache's own, bundle-derived list).
write_bundle_manifest() {
  tar -tzf "$1" | sed -n "s#^${ARCHIVE_DATASET_ROOT}/##p" | grep -v -e '/$' -e '^$' -e '/\._' -e '^\._' -e '\.DS_Store$' \
    | LC_ALL=C sort -u > "${DATASET_ROOT}/.dataset-manifest" || true
}

# The cache KEY carries this format (the `fmt2` segment of `datasets-fmt2-<tag>-<sha>` in every workflow
# and in the restore action): caches built before the two lists existed can never match, so the first
# run cold-builds and SAVES one with the metadata instead of re-downloading on every job forever.
DATASET_CACHE_FORMAT=fmt2

_dataset_restore_mode() { [ -n "${CI:-}" ] || [ "${CQLITE_DATASETS_RESTORE_TRACKED:-}" = "1" ]; }

# warm_cache_usable — the warm fast path may be taken only if the cache can be classified. (In restore
# mode the tracked files were already restored from the index, before this and before has_required_dataset.)
warm_cache_usable() {
  _dataset_restore_mode || return 0
  [ -s "${DATASET_ROOT}/.dataset-manifest" ] && return 0
  echo "NOTE: ${DATASET_ROOT} has no .dataset-manifest (a cache from before #4508); its stale files cannot be classified, so forcing a COLD restore" >&2
  return 1
}

# _dataset_safe_rel <rel> — a clean dataset-relative path that, resolved, stays under the dataset root.
_dataset_safe_rel() {
  local rel="$1" parent
  case "${rel}" in
    '' | /* | ../* | */../* | */.. | .. | ./* | */./* | */. | . | *[[:cntrl:]]*) return 1 ;;
  esac
  [ ! -L "${DATASET_ROOT}/${rel}" ] || return 1
  parent="$(cd "$(dirname "${DATASET_ROOT}/${rel}")" 2>/dev/null && pwd -P)" || return 1
  case "${parent}/" in "${DATASET_PHYS}/"*) return 0 ;; esac
  return 1
}

# _dataset_stale_lists — sets STALE_TWINS and STALE_EXTRAS (newline-separated, dataset-relative).
# Returns 0 classified, 2 not applicable (the dataset root is outside any git repo, so nothing is
# tracked), 1 a failure: an unclassifiable cache, a missing or stale bundle-tracked manifest, an
# unprotected guard state, a newline in a path, or ANY failed step. Every step is checked explicitly
# (this runs inside `||` lists, where errexit is OFF, so a failed sort must not yield a partial list).
# An existing-but-EMPTY tracked list is the empty set: a change that deletes every tracked dataset
# file must still have its cached twins removed.
_dataset_stale_lists() {
  local rc=0 work
  STALE_TWINS=""; STALE_EXTRAS=""
  [ "${TRACKED_GUARD_STATE:-}" = "out-of-repo" ] && return 2
  work="$(mktemp -d "${TMPDIR:-/tmp}/cqlite-ds-stale.XXXXXX")" || return 1
  _dataset_stale_lists_in "${work}" || rc=$?
  rm -rf "${work}"
  return "${rc}"
}

_dataset_stale_lists_in() {
  local w="$1" manifest="${DATASET_ROOT}/.dataset-manifest"
  local repo_manifest="${TRACKED_GUARD_REPO:-}/test-data/dataset-bundle-tracked.txt"
  [ -s "${manifest}" ] || return 1
  [ "${TRACKED_GUARD_STATE:-}" = "in-repo" ] && [ -f "${TRACKED_GUARD_LIST:-}" ] || return 1
  [ -f "${repo_manifest}" ] || { echo "ERROR: ${repo_manifest} is missing (generate it with test-data/scripts/gen-dataset-tracked-manifest.sh)" >&2; return 1; }
  [ "$(head -n 1 "${repo_manifest}")" = "# sha256=${SHA256_EXPECTED}" ] \
    || { echo "ERROR: ${repo_manifest} does not describe the pinned bundle ${SHA256_EXPECTED}; regenerate it in the re-pin PR" >&2; return 1; }
  # a newline inside a tracked path would split one NUL record into two lines: refuse, never guess
  [ "$(tr -cd '\n' < "${TRACKED_GUARD_LIST}" | wc -c | tr -d ' ')" = 0 ] || { echo "ERROR: a tracked dataset path contains a newline; refusing to classify" >&2; return 1; }
  (cd "${DATASET_ROOT}" && find . -type f -print0) > "${w}/disk0" || return 1
  [ "$(tr -cd '\n' < "${w}/disk0" | wc -c | tr -d ' ')" = 0 ] || { echo "ERROR: a file under ${DATASET_ROOT} has a newline in its name; refusing to classify" >&2; return 1; }

  tr '\0' '\n' < "${TRACKED_GUARD_LIST}" > "${w}/now0" || return 1
  sed "s#^${TRACKED_GUARD_REL}/##" "${w}/now0" > "${w}/now1" || return 1
  LC_ALL=C sort -u "${w}/now1" > "${w}/now" || return 1
  tr '\0' '\n' < "${w}/disk0" > "${w}/disk1" || return 1
  sed 's#^\./##' "${w}/disk1" > "${w}/disk2" || return 1
  LC_ALL=C sort -u "${w}/disk2" > "${w}/disk" || return 1
  tail -n +2 "${repo_manifest}" > "${w}/bt1" || return 1
  LC_ALL=C sort -u "${w}/bt1" > "${w}/bundle-tracked" || return 1
  cat "${w}/now" "${manifest}" > "${w}/known0" || return 1
  printf '%s\n' .dataset-pin .dataset-manifest .dataset-tracked-at-build >> "${w}/known0" || return 1
  LC_ALL=C sort -u "${w}/known0" > "${w}/known" || return 1
  # twins: in the bundle's tracked set, not tracked now, still on disk
  LC_ALL=C comm -23 "${w}/bundle-tracked" "${w}/now" > "${w}/twins0" || return 1
  LC_ALL=C comm -12 "${w}/disk" "${w}/twins0" > "${w}/twins" || return 1
  # extras: on disk, in neither git nor the bundle's file list
  LC_ALL=C comm -23 "${w}/disk" "${w}/known" > "${w}/extras" || return 1
  STALE_TWINS="$(cat "${w}/twins")" || return 1
  STALE_EXTRAS="$(cat "${w}/extras")" || return 1
}

# _dataset_remove_list <description> <newline-separated dataset-relative paths> — validate, remove, log.
# Returns 1 (after refusing) on any unsafe path: nothing outside the dataset root is ever deleted.
_dataset_remove_list() {
  local what="$1" list="$2" rel
  [ -n "${list}" ] || return 0
  DATASET_PHYS="$(cd "${DATASET_ROOT}" && pwd -P)" || return 1
  while IFS= read -r rel; do
    [ -n "${rel}" ] || continue
    if ! _dataset_safe_rel "${rel}"; then
      echo "ERROR: refusing to remove unsafe stale ${what} path '${rel}' (outside ${DATASET_ROOT}, a symlink, or not a clean relative path)" >&2
      return 1
    fi
    rm -f "${DATASET_ROOT}/${rel}" || return 1
    echo "REMOVED stale ${what}: ${rel}" >&2
  done <<<"${list}"
}

# warm_cache_reconcile — the warm fast path, after capture_tracked_dataset_files and warm_cache_usable
# (which, in restore mode, has already restored every tracked file from the index).
warm_cache_reconcile() {
  local drift rc
  if _dataset_restore_mode; then
    rc=0; _dataset_stale_lists || rc=$?
    if [ "${rc}" = 0 ]; then
      _dataset_remove_list "twin of a deleted/renamed tracked file" "${STALE_TWINS}" || exit 1
      _dataset_remove_list "extra (in neither git nor the bundle)" "${STALE_EXTRAS}" || exit 1
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
    echo "NOTE: stale files cannot be classified (no .dataset-manifest, or no matching test-data/dataset-bundle-tracked.txt)" >&2
  fi
}
