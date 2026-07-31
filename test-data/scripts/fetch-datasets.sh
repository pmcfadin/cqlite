#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v3 DATASET_ASSET=cassandra5-small-full-v3.5.tar.gz DATASET_SHA256=414195074f6df446a7381aad051af84158e9a021a6e2cd21cbc6c3ad0be1ba16 ./test-data/scripts/fetch-datasets.sh

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

# EVERY git invocation in this script goes through this wrapper (issue #2878). An
# exported GIT_DIR/GIT_WORK_TREE — routine inside git hooks and on some CI runners
# — makes `rev-parse --show-toplevel` report the CURRENT DIRECTORY as a work-tree
# root, which made the tracked-fixture guard refuse EVERY fetch while citing a
# work tree that is not one. Scrubbing the two variables per invocation keeps the
# fix scoped to this script's own probes and leaves the caller's environment
# untouched.
guard_git() {
  env -u GIT_DIR -u GIT_WORK_TREE git "$@"
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

  mkdir -p "${parent}"
  parent_abs="$(cd "${parent}" && pwd -P)"
  [ "${parent_abs}" != "/" ] \
    || fail_unsafe_dataset_root "refusing to replace a top-level /datasets directory"

  abs_root="${parent_abs}/${base}"

  if repo_root="$(guard_git rev-parse --show-toplevel 2>/dev/null)"; then
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

# ---------------------------------------------------------------------------
# Tracked-fixture guard (issue #2878)
#
# `rm -rf "${DATASET_ROOT}"` below is unconditionally destructive, and the pinned
# archive is NOT a superset of the checkout: ~875 files under test-data/datasets
# are git-TRACKED (162 JSONL sstabledump goldens, force-added byte-parity *.db
# references, the 4 commitlog fixtures from #2389) and several of them ship in no
# archive at all. Deleting them and extracting does not bring them back: the gate
# then FAILs core/cli tests on a pristine main and leaves the checkout dirty with
# stageable deletions of tracked fixtures (data loss by accident).
#
# So we CAPTURE the tracked-file list from git BEFORE the destructive step and
# restore exactly that captured list afterwards. Four rules follow from the
# pre-#2878 defect, whose restore path never ran:
#   * NOT CI-gated. The local/agent box (CI unset) is the destructive arm.
#   * NEVER a silent bail. If we cannot determine whether the dataset dir holds
#     tracked files, we REFUSE to run `rm -rf` rather than guess — see
#     refuse_unprotected_dataset_root.
#   * NEVER destroy the thing that makes restoration possible. A target that IS a
#     repository root, or that CONTAINS a nested checkout, would take `.git` with
#     it and void the guard's whole premise (that the index survives) — refuse.
#   * The window between `rm -rf` and the restore must be CRASH-SAFE. An abort in
#     it (bad archive, tar failure, ENOSPC on the mv, SIGINT mid-extract) used to
#     leave every fixture permanently deleted while the error blamed the archive,
#     so the EXIT/INT/TERM/HUP path restores what it can and says so. Only
#     SIGKILL can outrun that; the next run's `missing-only` pre-flight is the
#     backstop for it.
# ---------------------------------------------------------------------------

# State published by capture_tracked_dataset_files, consumed by
# restore_tracked_dataset_files. Only `in-repo` makes the restore do work; a
# mislabelled state on a data-destroying path is how the original bug hid, so
# every value is enumerated here:
#   unset        capture has not run yet (restore is a no-op)
#   pending      capture STARTED but did not reach a verdict (it refuses rather
#                than returning in that case, so this value escapes only if a new
#                early-return path is added — treated as "no restore possible")
#   in-repo      the dataset dir lives in a git work tree; REPO/REL/LIST/COUNT are
#                valid and the restore uses them
#   out-of-repo  provably outside any git work tree; there is nothing to protect
#   unprotected  the guard COULD NOT protect the dir but the operator overrode it
#                with CQLITE_DATASETS_ALLOW_UNPROTECTED=1; the restore is a no-op
#                and tracked files may be lost. Distinct from out-of-repo, which
#                is a proof of safety rather than an accepted risk.
TRACKED_GUARD_STATE=unset
TRACKED_GUARD_REPO=""
TRACKED_GUARD_REL=""
TRACKED_GUARD_LIST=""
TRACKED_GUARD_COUNT=0
# Set to 1 immediately before the `rm -rf`, back to 0 once the post-extraction
# restore has verified the tracked subtree clean. While it is 1, an abort MUST
# restore on the way out (see cleanup_fetch_temporaries).
TRACKED_GUARD_DESTRUCTIVE_STARTED=0

# Escape hatch for an environment that genuinely cannot run the guard (no git
# binary, exotic mount). Loud, opt-in, never the default — and it unlocks ONLY the
# GUARD-AVAILABILITY class of refusal, never a STRUCTURAL one. See the two refuse_*
# functions below.
TRACKED_GUARD_ALLOW_UNPROTECTED="${CQLITE_DATASETS_ALLOW_UNPROTECTED:-}"

cleanup_tracked_guard_list() {
  [ -n "${TRACKED_GUARD_LIST}" ] && rm -f "${TRACKED_GUARD_LIST}"
  TRACKED_GUARD_LIST=""
  return 0
}

# ---------------------------------------------------------------------------
# TWO CLASSES OF REFUSAL (issue #2878). The distinction is whether anything would
# be left to restore FROM after the `rm -rf`:
#
#   STRUCTURAL — the deletion would destroy the git repository itself (target is a
#     repository root, plain or bare; contains a nested repository; is an ancestor
#     of the work tree; resolves to the work-tree root; is $HOME or /) or an
#     unrecoverable working state (unmerged/conflicted index entries, which
#     `git restore` cannot rebuild). The index IS the guard's restore source, so
#     losing it makes recovery impossible by construction. NON-OVERRIDABLE: no
#     environment variable unlocks these, because there is no legitimate reason to
#     delete a repository in order to unpack a dataset into it. (The $HOME and /
#     spellings are enforced earlier, unconditionally, by
#     canonicalize_dataset_root/fail_unsafe_dataset_root.)
#
#   GUARD-AVAILABILITY — the guard cannot TELL whether tracked files are at risk
#     (no git binary, classification unresolved, `ls-files` unusable). The worst
#     case here is losing tracked FILES, which a re-checkout recovers, and a user
#     may knowingly fetch into a plain directory on a box without git — so this
#     class alone is unlockable with CQLITE_DATASETS_ALLOW_UNPROTECTED=1.
#
# Ordering rule that makes the split real: every git-free STRUCTURAL check runs
# BEFORE any overridable bail, so the hatch can never skip one by way of an
# earlier availability escape.
# ---------------------------------------------------------------------------
refuse_structural_dataset_root() {
  local reason="$1"
  echo "ERROR: refusing to replace '${DATASET_ROOT}': ${reason}" >&2
  echo "ERROR: this is a STRUCTURAL refusal (issue #2878): the deletion would destroy a git" >&2
  echo "ERROR: repository or an unrecoverable working state, leaving NOTHING to restore from." >&2
  echo "ERROR: It is NOT overridable — CQLITE_DATASETS_ALLOW_UNPROTECTED does not unlock it." >&2
  echo "ERROR: Point CQLITE_DATASETS_ROOT at a plain directory instead." >&2
  exit 1
}

refuse_unprotected_dataset_root() {
  local reason="$1"
  if [ "${TRACKED_GUARD_ALLOW_UNPROTECTED}" = "1" ]; then
    echo "WARNING: cannot protect git-tracked files under '${DATASET_ROOT}': ${reason}" >&2
    echo "WARNING: proceeding anyway because CQLITE_DATASETS_ALLOW_UNPROTECTED=1; tracked fixtures may be DELETED" >&2
    echo "WARNING: (the override unlocks ONLY this guard-availability case; a repository-destroying" >&2
    echo "WARNING: refusal is structural and stays refused regardless of this variable)" >&2
    TRACKED_GUARD_STATE=unprotected
    return 0
  fi
  echo "ERROR: refusing to replace '${DATASET_ROOT}': ${reason}" >&2
  echo "ERROR: this script deletes that directory, and it may contain git-tracked reference files" >&2
  echo "ERROR: (issue #2878). Point CQLITE_DATASETS_ROOT at a directory outside any git checkout," >&2
  echo "ERROR: install git so the tracked files can be captured and restored, or set" >&2
  echo "ERROR: CQLITE_DATASETS_ALLOW_UNPROTECTED=1 to accept the loss of tracked files." >&2
  exit 1
}

# Physical absolute path of a target that need not exist yet: resolve the nearest
# EXISTING ancestor with `cd -P`/`pwd -P` (which collapses symlinks and `..`),
# then re-append the not-yet-existing remainder. A raw string prefix compare
# against an unresolved path is exactly what silently skipped the pre-#2878
# restore.
physical_path() {
  local target="$1" suffix="" head resolved
  case "${target}" in
    /*) ;;
    "") return 1 ;;
    *) target="${PWD}/${target}" ;;
  esac
  head="${target}"
  while [ ! -e "${head}" ]; do
    case "${head}" in
      /|.|"") return 1 ;;
    esac
    suffix="/$(basename "${head}")${suffix}"
    head="$(dirname "${head}")"
  done
  if [ -d "${head}" ]; then
    resolved="$(cd -P "${head}" 2>/dev/null && pwd -P)" || return 1
  else
    local head_parent head_base
    head_parent="$(dirname "${head}")"
    head_base="$(basename "${head}")"
    resolved="$(cd -P "${head_parent}" 2>/dev/null && pwd -P)" || return 1
    resolved="${resolved%/}/${head_base}"
  fi
  printf '%s\n' "${resolved%/}${suffix}"
}

# Nearest existing DIRECTORY ancestor of an absolute path (the `git -C` probe
# point: DATASET_ROOT itself may not exist yet).
nearest_existing_dir() {
  local head="$1"
  while [ ! -d "${head}" ]; do
    case "${head}" in
      /|.|"") printf '/\n'; return 0 ;;
    esac
    head="$(dirname "${head}")"
  done
  printf '%s\n' "${head}"
}

# Component-wise containment: prints the child's path RELATIVE to the parent when
# the child is the parent or lives under it, else fails. Both sides must already
# be physical absolute paths. Component-wise (not string-prefix) so
# /repo/test-data-foo is NOT treated as inside /repo/test-data.
path_relative_inside() {
  local parent="${1%/}" child="${2%/}"
  [ -n "${parent}" ] || parent="/"
  if [ "${child}" = "${parent}" ]; then
    printf '.\n'
    return 0
  fi
  case "${parent}" in
    /) printf '%s\n' "${child#/}"; return 0 ;;
  esac
  case "${child}" in
    "${parent}"/*) printf '%s\n' "${child#"${parent}/"}"; return 0 ;;
    *) return 1 ;;
  esac
}

# Prints WHY the given directory is itself a git repository, else fails. Purely a
# filesystem probe (no git binary needed), and it must recognise a BARE repository
# too: `git init --bare .../datasets` leaves NO `.git` entry, so an
# `-e "$dir/.git"` test alone classified it as an ordinary directory and the
# `rm -rf` deleted the whole repository while REPORTING SUCCESS. A bare repo has
# `HEAD` + `objects/` at its top level, and its `config` declares `bare = true`;
# either signature is enough, and over-refusing here is the safe direction.
git_repository_reason() {
  local dir="$1"
  if [ -e "${dir}/.git" ]; then
    printf "'%s/.git' exists\n" "${dir}"
    return 0
  fi
  if [ -f "${dir}/HEAD" ] && [ -d "${dir}/objects" ]; then
    printf "it has the layout of a BARE git repository ('HEAD' + 'objects/')\n"
    return 0
  fi
  if [ -f "${dir}/config" ] \
    && grep -qE '^[[:space:]]*bare[[:space:]]*=[[:space:]]*true' "${dir}/config" 2>/dev/null; then
    printf "its 'config' declares 'bare = true'\n"
    return 0
  fi
  return 1
}

# Prints the path of the first git repository found STRICTLY BENEATH the given
# directory (depth >= 2 — a nested checkout, submodule or bare mirror, not the
# directory's own repository), else fails. `rm -rf` on such a target destroys a
# repository whose index we do not have, so restoration is impossible and the
# guard's premise is void. Uses only `find` so it applies whether or not git is
# installed and regardless of whether the target is itself inside a checkout. One
# NUL-safe walk, matching the two entries that mark a repository root (`.git` for
# a checkout, `HEAD` for a bare one); ~16ms over the real dataset tree, which
# contains neither.
nested_repo_under() {
  local dir="$1" candidate parent
  [ -d "${dir}" ] || return 1
  while IFS= read -r -d '' candidate; do
    case "${candidate##*/}" in
      .git)
        printf '%s\n' "${candidate}"
        return 0
        ;;
      HEAD)
        parent="$(dirname "${candidate}")"
        if git_repository_reason "${parent}" >/dev/null; then
          printf '%s\n' "${parent}"
          return 0
        fi
        ;;
    esac
  done < <(find "${dir}" -mindepth 2 \( -name .git -o -name HEAD \) -print0 2>/dev/null)
  return 1
}

# True when some ancestor of the path (or the path itself) holds a .git entry —
# a git-free filesystem probe used only to decide whether an unprotectable
# dataset dir is plausibly inside a checkout (i.e. whether to refuse).
ancestor_has_git_dir() {
  local head="$1"
  while :; do
    [ -e "${head}/.git" ] && return 0
    case "${head}" in
      /|.|"") return 1 ;;
    esac
    head="$(dirname "${head}")"
  done
}

# Capture the git-tracked files under DATASET_ROOT. MUST be called before any
# destructive step; safe to call repeatedly (re-captures).
capture_tracked_dataset_files() {
  cleanup_tracked_guard_list
  TRACKED_GUARD_STATE=pending
  TRACKED_GUARD_REPO=""
  TRACKED_GUARD_REL=""
  TRACKED_GUARD_COUNT=0

  local dataset_abs probe repo_root repo_root_phys rel nested repo_reason
  # STRUCTURAL: an unresolvable path is refused outright rather than sent through
  # the hatch — with no physical path, NONE of the safety checks below can run, so
  # an overridable bail here would silently unlock all of them.
  if ! dataset_abs="$(physical_path "${DATASET_ROOT}")"; then
    refuse_structural_dataset_root "could not resolve a physical path for it, so no safety check can be evaluated"
    return 0
  fi

  # STRUCTURAL: the target must never BE a repository, nor CONTAIN one — `rm -rf`
  # would take the repository with it, and an index we deleted cannot restore
  # anything. Both are git-free filesystem probes, deliberately placed before every
  # overridable bail so the hatch can never skip them, and they cover an
  # out-of-repo target too (someone else's checkout is just as unrecoverable).
  if repo_reason="$(git_repository_reason "${dataset_abs}")"; then
    refuse_structural_dataset_root "'${dataset_abs}' is itself a git repository (${repo_reason%$'\n'}) — deleting it would destroy the repository, not just fixtures"
    return 0
  fi
  if nested="$(nested_repo_under "${dataset_abs}")"; then
    refuse_structural_dataset_root "'${dataset_abs}' contains a nested git repository at '${nested}' — deleting it would destroy that checkout irrecoverably"
    return 0
  fi

  if ! command -v git >/dev/null 2>&1; then
    if ancestor_has_git_dir "${dataset_abs}"; then
      refuse_unprotected_dataset_root "git is not installed but '${dataset_abs}' is inside a git checkout"
      return 0
    fi
    TRACKED_GUARD_STATE=out-of-repo
    return 0
  fi

  probe="$(nearest_existing_dir "${dataset_abs}")"
  if ! repo_root="$(guard_git -C "${probe}" rev-parse --show-toplevel 2>/dev/null)" || [ -z "${repo_root}" ]; then
    if ancestor_has_git_dir "${dataset_abs}"; then
      refuse_unprotected_dataset_root "'${dataset_abs}' looks like it is inside a git checkout, but 'git -C ${probe} rev-parse --show-toplevel' reported no work tree"
      return 0
    fi
    TRACKED_GUARD_STATE=out-of-repo
    return 0
  fi

  if ! repo_root_phys="$(physical_path "${repo_root}")"; then
    refuse_unprotected_dataset_root "could not resolve a physical path for the enclosing repository '${repo_root}'"
    return 0
  fi

  # The probe IS inside this work tree, and dataset_abs only appends components
  # to it — so a containment failure here means the two paths disagree in a way
  # we do not understand. Refusing is mandatory: a silent skip is the #2878 bug.
  if ! rel="$(path_relative_inside "${repo_root_phys}" "${dataset_abs}")"; then
    # dataset_abs is not inside the work tree git reported. If the work tree is
    # inside IT, the `rm -rf` would delete the whole repository — refuse.
    if path_relative_inside "${dataset_abs}" "${repo_root_phys}" >/dev/null; then
      refuse_structural_dataset_root "'${dataset_abs}' is an ANCESTOR of the git work tree at '${repo_root_phys}' — deleting it would destroy the repository"
      return 0
    fi
    refuse_unprotected_dataset_root "could not decide whether '${dataset_abs}' is inside the git work tree at '${repo_root_phys}' (probed from '${probe}')"
    return 0
  fi

  # STRUCTURAL: rel="." means the dataset dir IS the work-tree root
  # (git_repository_reason above covers the common shapes; this also catches a work
  # tree whose git dir lives elsewhere, e.g. a linked worktree or core.worktree).
  if [ "${rel}" = "." ]; then
    refuse_structural_dataset_root "'${dataset_abs}' is the root of the git work tree at '${repo_root_phys}' — deleting it would destroy the repository, not just fixtures"
    return 0
  fi

  TRACKED_GUARD_LIST="$(mktemp "${TMPDIR:-/tmp}/cqlite-tracked-datasets.XXXXXX")"
  # --deduplicate (git >= 2.31): a path in a merge conflict is otherwise listed
  # once per stage, duplicating the count and the restore pathspecs. Older git
  # lacks the option, so retry without it — duplicates only cost work.
  if ! guard_git -C "${repo_root_phys}" ls-files -z --deduplicate -- ":(literal)${rel}" >"${TRACKED_GUARD_LIST}" 2>/dev/null; then
    if ! guard_git -C "${repo_root_phys}" ls-files -z -- ":(literal)${rel}" >"${TRACKED_GUARD_LIST}"; then
      cleanup_tracked_guard_list
      refuse_unprotected_dataset_root "'git ls-files' failed for '${rel}' in '${repo_root_phys}'"
      return 0
    fi
  fi

  # STRUCTURAL: mid-merge is not a state in which to nuke and rebuild a fixture
  # tree. `git restore --worktree` CANNOT rebuild an unmerged (conflicted) path —
  # there is no single stage to restore from — so after the `rm -rf` the restore
  # would fail on exactly those paths, the abort trap would retry the same failing
  # call, and the working-tree content would be gone for good. Deduplicating the
  # captured list (above) makes the list correct but does not make those entries
  # restorable, so detecting them BEFORE the destructive step and refusing is the
  # only safe answer.
  local unmerged unmerged_paths
  if ! unmerged="$(guard_git -C "${repo_root_phys}" ls-files -u -- ":(literal)${rel}")"; then
    cleanup_tracked_guard_list
    refuse_unprotected_dataset_root "'git ls-files -u' failed for '${rel}' in '${repo_root_phys}', so unmerged paths cannot be ruled out"
    return 0
  fi
  if [ -n "${unmerged}" ]; then
    unmerged_paths="$(printf '%s\n' "${unmerged}" | awk -F'\t' 'NF > 1 { print $2 }' | sort -u | head -3 | tr '\n' ' ' || true)"
    cleanup_tracked_guard_list
    refuse_structural_dataset_root "'${rel}' in '${repo_root_phys}' has UNMERGED (conflicted) index entries (e.g. ${unmerged_paths%% }) — 'git restore' cannot rebuild a conflicted path, so the deletion would be permanent. Resolve or abort the merge first"
    return 0
  fi

  # Count NUL-terminated entries exactly (a filename may contain a newline).
  local count=0 rel_path
  while IFS= read -r -d '' rel_path; do
    count=$((count + 1))
  done <"${TRACKED_GUARD_LIST}"

  TRACKED_GUARD_REPO="${repo_root_phys}"
  TRACKED_GUARD_REL="${rel}"
  TRACKED_GUARD_COUNT="${count}"
  TRACKED_GUARD_STATE=in-repo
  return 0
}

# Restore the captured tracked files from the index.
#   missing-only  only files that are absent on disk (pre-flight repair of an
#                 earlier run's damage, and the abort path; never clobbers a
#                 local modification)
#   all           every captured file (post-extraction, after rm -rf)
# In `all` mode the tracked subtree MUST end clean, else this RETURNS NON-ZERO:
# a restore that silently no-ops is otherwise indistinguishable from one that
# worked (issue #2878 acceptance oracle).
# Returns 1 rather than calling `exit` so it is safe to call from a trap, where an
# `exit` would rewrite the aborting run's status. Callers on the main path turn a
# non-zero return into `exit 1` themselves.
restore_tracked_dataset_files() {
  local mode="$1"
  [ "${TRACKED_GUARD_STATE}" = "in-repo" ] || return 0
  [ -n "${TRACKED_GUARD_LIST}" ] && [ -s "${TRACKED_GUARD_LIST}" ] || return 0

  local -a pathspecs=()
  local rel_path
  while IFS= read -r -d '' rel_path; do
    if [ "${mode}" = "missing-only" ] && [ -e "${TRACKED_GUARD_REPO}/${rel_path}" ]; then
      continue
    fi
    pathspecs+=( ":(literal)${rel_path}" )
  done <"${TRACKED_GUARD_LIST}"

  if [ "${#pathspecs[@]}" -gt 0 ]; then
    echo "Restoring ${#pathspecs[@]} git-tracked file(s) under ${TRACKED_GUARD_REL} (of ${TRACKED_GUARD_COUNT} tracked; mode=${mode})"
    # Batch so a very large fixture set cannot hit ARG_MAX.
    local batch=400 i
    for (( i = 0; i < ${#pathspecs[@]}; i += batch )); do
      # Restore from the INDEX (not HEAD): the index is what `git ls-files`
      # enumerated, so this reproduces the checkout's intended content and cannot
      # clobber a developer's STAGED edit to a tracked reference file.
      if ! guard_git -C "${TRACKED_GUARD_REPO}" restore --worktree -- "${pathspecs[@]:i:batch}"; then
        echo "ERROR: failed to restore git-tracked files under ${TRACKED_GUARD_REL} in ${TRACKED_GUARD_REPO} (issue #2878)" >&2
        return 1
      fi
    done
  fi

  # Post-condition (only meaningful for the post-destruction restore): no tracked
  # file under the dataset dir may be left deleted or modified relative to the
  # index. `git diff` isolates exactly that — untracked extracted content is
  # expected and is .gitignore's business, and a pre-existing STAGED change is
  # not damage this script caused.
  if [ "${mode}" = "all" ]; then
    local dirty
    if ! dirty="$(guard_git -C "${TRACKED_GUARD_REPO}" diff --name-status -- ":(literal)${TRACKED_GUARD_REL}")"; then
      echo "ERROR: could not verify tracked-file integrity under ${TRACKED_GUARD_REL} (issue #2878)" >&2
      return 1
    fi
    if [ -n "${dirty}" ]; then
      echo "ERROR: dataset fetch left git-tracked files under ${TRACKED_GUARD_REL} deleted or modified (issue #2878):" >&2
      printf '%s\n' "${dirty}" | head -20 >&2
      return 1
    fi
  fi
  return 0
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

EXTRACT_TMP=""
# ONE exit path for temporaries AND for the crash-safe restore (issue #2878
# BLOCKER 1). Any abort between the `rm -rf` and the post-extraction restore —
# a tarball with an unexpected layout (the explicit `exit 1` below), a tar
# failure, ENOSPC on the mv, Ctrl-C mid-extract of a multi-GB archive — used to
# leave every tracked fixture PERMANENTLY deleted while the error message blamed
# the archive. So while TRACKED_GUARD_DESTRUCTIVE_STARTED=1 this restores what it
# can on the way out, and says so.
#
# Ordering and status rules, both load-bearing:
#   * the restore runs BEFORE cleanup_tracked_guard_list — the captured list is
#     its input, so deleting it first would silently restore nothing;
#   * the incoming $? is captured first and re-asserted with `exit`, so neither a
#     failing cleanup command (under `set -e`) nor the restore itself can rewrite
#     the aborting run's status — an abort stays an abort, a success stays 0.
# Only SIGKILL outruns this; the next run's `missing-only` pre-flight covers it.
cleanup_fetch_temporaries() {
  local rc=$?
  # IGNORE further signals for the duration of the cleanup. Without this the
  # INT/TERM/HUP traps stay live inside this one, so a second Ctrl-C during
  # recovery re-enters, `exit`s out of a PARTIALLY completed restore, and prints
  # none of the messages below — a half-restored fixture tree, silently.
  trap '' INT TERM HUP
  # Only `in-repo` has a captured list to restore from; any other state (incl. the
  # CQLITE_DATASETS_ALLOW_UNPROTECTED override's `unprotected`) must NOT print a
  # restoration claim it cannot back.
  if [ "${TRACKED_GUARD_DESTRUCTIVE_STARTED}" = "1" ] && [ "${TRACKED_GUARD_STATE}" = "in-repo" ]; then
    TRACKED_GUARD_DESTRUCTIVE_STARTED=0
    echo "WARNING: dataset fetch aborted (status ${rc}) after '${DATASET_ROOT}' was deleted;" >&2
    echo "WARNING: restoring the git-tracked reference fixtures it contained (issue #2878)" >&2
    # `all`, not `missing-only`: an abort after the extraction landed can leave a
    # tracked reference file present but overwritten by the archive's stale copy,
    # and this mode also VERIFIES the subtree ends clean.
    if restore_tracked_dataset_files all; then
      echo "WARNING: git-tracked fixtures under ${TRACKED_GUARD_REL:-${DATASET_ROOT}} restored; the archive content is NOT present — re-run the fetch" >&2
    else
      echo "ERROR: could not restore git-tracked fixtures under ${TRACKED_GUARD_REL:-${DATASET_ROOT}};" >&2
      echo "ERROR: recover with: git -C '${TRACKED_GUARD_REPO:-.}' restore --worktree -- '${TRACKED_GUARD_REL:-test-data/datasets}'" >&2
    fi
  fi
  [ -n "${EXTRACT_TMP}" ] && rm -rf "${EXTRACT_TMP}"
  cleanup_tracked_guard_list
  exit "${rc}"
}
trap cleanup_fetch_temporaries EXIT
# Signals must take the same path (a bare signal default would skip the EXIT trap
# in some shells and always skips the restore). Exiting with the conventional
# 128+signal status runs the EXIT trap above exactly once.
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

# Pre-flight repair: a previous run (or a pre-#2878 run, or a SIGKILLed one) may
# have left tracked fixtures deleted. Restore only the MISSING ones so an
# intentional local edit to a tracked reference file survives, then let
# has_required_dataset judge.
capture_tracked_dataset_files
restore_tracked_dataset_files missing-only || exit 1

if has_required_dataset; then
  write_pin
  echo "Dataset ${ASSET} (tag ${TAG}) already present in ${DATASET_ROOT}; skipping download"
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

# Re-capture immediately before the destructive step so the restored list is
# exactly what existed at deletion time (issue #2878 AC1: capture, never
# re-derive after the fact). capture_* exits rather than returning when it cannot
# determine whether tracked files are at risk.
capture_tracked_dataset_files

# From here until the restore below verifies the tracked subtree clean, an abort
# on ANY path must restore on the way out — see cleanup_fetch_temporaries.
TRACKED_GUARD_DESTRUCTIVE_STARTED=1

rm -rf "${DATASET_ROOT}"
if [ "${DATASET_ROOT}" = "${ARCHIVE_DATASET_ROOT}" ]; then
  tar -xzf "${ASSET_PATH}" -C . --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'
else
  EXTRACT_TMP="$(mktemp -d)"
  tar -xzf "${ASSET_PATH}" -C "${EXTRACT_TMP}" --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'

  if [ ! -d "${EXTRACT_TMP}/${ARCHIVE_DATASET_ROOT}" ]; then
    echo "ERROR: dataset archive did not contain ${ARCHIVE_DATASET_ROOT}" >&2
    exit 1
  fi

  mkdir -p "$(dirname "${DATASET_ROOT}")"
  mv "${EXTRACT_TMP}/${ARCHIVE_DATASET_ROOT}" "${DATASET_ROOT}"
fi

# Undo the rm -rf for every captured tracked file, and hard-fail if the tracked
# subtree is not clean afterwards (issue #2878). Only once that verification has
# passed is the destructive window closed.
restore_tracked_dataset_files all || exit 1
TRACKED_GUARD_DESTRUCTIVE_STARTED=0

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
