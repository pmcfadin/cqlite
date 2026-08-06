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

# EVERY git invocation in this script goes through this wrapper (issue #2878).
#
# POSTURE: clear the ENTIRE `GIT_*` namespace, rather than blacklisting names.
# Inherited git environment is routine (hooks, `rebase --exec`, `filter-branch`,
# some CI runners) and it can redirect every input this guard depends on:
#   * GIT_DIR/GIT_WORK_TREE made `rev-parse --show-toplevel` report the CURRENT
#     DIRECTORY as a work-tree root, so the guard refused EVERY fetch;
#   * a VALID but FOREIGN GIT_INDEX_FILE was worse than that — `ls-files` read the
#     other index and captured ZERO files, then the post-extract `git diff`
#     verification consulted that same wrong index and reported CLEAN, so tracked
#     fixtures were deleted and the run declared success. That is #2878 itself
#     reproduced through another door: an oracle pointed at the wrong index.
#   * GIT_OBJECT_DIRECTORY / GIT_ALTERNATE_OBJECT_DIRECTORIES / GIT_COMMON_DIR
#     redirect object and admin storage the same way.
# A `-u` blacklist is an ever-growing list that fails silently the day git adds
# another variable, and NONE of this script's four operations (rev-parse, ls-files,
# restore --worktree, diff) needs any GIT_* input, so clearing the namespace has no
# legitimate cost. Where it is visible, it fails LOUDLY (e.g. a checkout that
# needed a `safe.directory` supplied via GIT_CONFIG_GLOBAL now errors instead of
# silently mis-reading), and loud beats silent data loss.
# The subshell keeps the caller's environment untouched.
guard_git() {
  (
    # `${!GIT_@}` enumerates every GIT_-prefixed variable in scope, inherited ones
    # included; unquoted on purpose (names cannot word-split), and `unset` with no
    # arguments is a no-op.
    # shellcheck disable=SC2086
    unset ${!GIT_@} 2>/dev/null || true
    git "$@"
  )
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
# Directory holding the guard's OWN state files, PROVEN to lie outside the deletion
# target — see resolve_guard_state_dir. TMPDIR is a knob people really do set (this
# repo's own test harness sets it), and a capture list stored under DATASET_ROOT is
# eaten by the very `rm -rf` it exists to undo.
TRACKED_GUARD_STATE_DIR=""

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

# Prints the git ADMINISTRATIVE directory at or above the given path, else fails.
# A target inside git's own storage (`/repo/.git/…`, `/mirror.git/objects/…`, a
# submodule's `.git/modules/…`, a linked worktree's `.git/worktrees/…`) holds no
# tracked files, so it used to classify as "nothing to protect" — and the `rm -rf`
# then deleted the object store that the restore strategy itself depends on: the
# guard destroying its own recovery source while reporting success. Filesystem-only
# (a directory named `.git`, or the admin layout HEAD+objects/) so it fires even
# when git is absent — a missing git binary must never downgrade a STRUCTURAL
# refusal into an overridable one.
git_admin_dir_at_or_above() {
  local head="$1"
  while :; do
    if [ -d "${head}" ] \
      && { [ "${head##*/}" = ".git" ] || { [ -f "${head}/HEAD" ] && [ -d "${head}/objects" ]; }; }; then
      printf '%s\n' "${head}"
      return 0
    fi
    case "${head}" in
      /|.|"") return 1 ;;
    esac
    head="$(dirname "${head}")"
  done
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
  local dir="$1" candidate parent scan_out scan_err detail
  [ -d "${dir}" ] || return 1

  # The traversal's EXIT STATUS is load-bearing. Discarding it (process substitution
  # plus 2>/dev/null) made a FAILED scan — a permission error, an I/O error, an
  # unsupported option — indistinguishable from a clean one, silently downgrading a
  # structural refusal into "safe to delete": #2878's original silent bail in a new
  # place. So the output goes to a scratch file (outside the deletion target) and a
  # non-zero find is reported as a distinct "could not complete" verdict.
  if ! scan_out="$(mktemp "${TRACKED_GUARD_STATE_DIR:-${TMPDIR:-/tmp}}/cqlite-nested-scan.XXXXXX")"; then
    printf "the nested-repository scan of '%s' could not start (no scratch file)\n" "${dir}"
    return 2
  fi
  scan_err="${scan_out}.err"
  if ! find "${dir}" -mindepth 2 \( -name .git -o -name HEAD \) -print0 \
    >"${scan_out}" 2>"${scan_err}"; then
    detail="$(tr '\n' ' ' <"${scan_err}" 2>/dev/null | cut -c1-200)"
    rm -f "${scan_out}" "${scan_err}"
    printf "the nested-repository scan of '%s' FAILED to complete (find: %s)\n" \
      "${dir}" "${detail:-no diagnostic}"
    return 2
  fi
  rm -f "${scan_err}"

  while IFS= read -r -d '' candidate; do
    case "${candidate##*/}" in
      .git)
        rm -f "${scan_out}"
        printf '%s\n' "${candidate}"
        return 0
        ;;
      HEAD)
        parent="$(dirname "${candidate}")"
        if git_repository_reason "${parent}" >/dev/null; then
          rm -f "${scan_out}"
          printf '%s\n' "${parent}"
          return 0
        fi
        ;;
    esac
  done <"${scan_out}"
  rm -f "${scan_out}"
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

# Prints the first writable directory PROVEN to lie outside the deletion target (and
# outside git's admin storage) among TMPDIR, /tmp and HOME, else fails. Uses the same
# component-wise containment test as the structural checks, so "outside" is proven
# rather than assumed.
resolve_guard_state_dir() {
  local dataset_abs="$1" candidate candidate_phys
  for candidate in "${TMPDIR:-}" /tmp "${HOME:-}"; do
    [ -n "${candidate}" ] || continue
    [ -d "${candidate}" ] && [ -w "${candidate}" ] || continue
    candidate_phys="$(physical_path "${candidate}")" || continue
    # Inside the tree we are about to delete? Then the state would be deleted too.
    path_relative_inside "${dataset_abs}" "${candidate_phys}" >/dev/null && continue
    git_admin_dir_at_or_above "${candidate_phys}" >/dev/null && continue
    printf '%s\n' "${candidate_phys}"
    return 0
  done
  return 1
}

# Self-consistency: the script KNOWS it captured N>0 entries, so a capture list that
# has vanished or been emptied is provably an ERROR and must NEVER be read as
# "nothing to restore" — that silent no-op IS #2878's original defect, and it is
# exactly what a TMPDIR at or below DATASET_ROOT used to produce. Checked by
# restore_tracked_dataset_files on every path, the abort path included.
guard_list_is_consistent() {
  [ "${TRACKED_GUARD_STATE}" = "in-repo" ] || return 0
  [ "${TRACKED_GUARD_COUNT}" -gt 0 ] || return 0
  if [ -n "${TRACKED_GUARD_LIST}" ] && [ -s "${TRACKED_GUARD_LIST}" ]; then
    return 0
  fi
  echo "ERROR: the captured list of ${TRACKED_GUARD_COUNT} git-tracked file(s) under '${TRACKED_GUARD_REL}' is MISSING or EMPTY (expected at '${TRACKED_GUARD_LIST:-<unset>}')" >&2
  echo "ERROR: — refusing to report a no-op restore as success (issue #2878)" >&2
  echo "ERROR: recover with: git -C '${TRACKED_GUARD_REPO:-.}' restore --worktree -- '${TRACKED_GUARD_REL:-test-data/datasets}'" >&2
  return 1
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
  local admin_dir
  if admin_dir="$(git_admin_dir_at_or_above "${dataset_abs}")"; then
    refuse_structural_dataset_root "'${dataset_abs}' is at or beneath git's administrative storage '${admin_dir}' — deleting it would corrupt the repository and destroy the object store this guard restores FROM"
    return 0
  fi

  # Establish where the guard's own state may live BEFORE anything else needs it, so
  # neither the capture list, the nested-repository scan, nor the extraction staging
  # area can sit inside the tree the `rm -rf` removes.
  if ! TRACKED_GUARD_STATE_DIR="$(resolve_guard_state_dir "${dataset_abs}")"; then
    refuse_structural_dataset_root "no writable temporary directory (of TMPDIR, /tmp, HOME) could be PROVEN to lie outside '${dataset_abs}', so this guard's own capture list would be deleted by the very 'rm -rf' it exists to undo"
    return 0
  fi

  # STRUCTURAL: a repository NESTED beneath the target. Runs after the state dir is
  # resolved because the scan needs a scratch file outside the deletion target. Exit
  # code 2 means the scan could not COMPLETE — refused just as hard as a positive
  # find, because an incomplete traversal that reads as "clean" is a fail-OPEN on a
  # data-destroying path.
  local nested_rc=0
  nested="$(nested_repo_under "${dataset_abs}")" || nested_rc=$?
  case "${nested_rc}" in
    0)
      refuse_structural_dataset_root "'${dataset_abs}' contains a nested git repository at '${nested}' — deleting it would destroy that checkout irrecoverably"
      return 0
      ;;
    2)
      refuse_structural_dataset_root "${nested} — a nested git repository therefore cannot be ruled out, and deleting one would be irrecoverable"
      return 0
      ;;
  esac

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

  # STRUCTURAL, git-informed complement to git_admin_dir_at_or_above: ASK git where
  # its administrative directories are, so a `.git`-FILE indirection (linked
  # worktrees, submodules) is covered even though the admin storage lives outside
  # the work tree and has no recognisable layout at the target's ancestors.
  local admin_kind admin_raw admin_phys
  for admin_kind in absolute-git-dir git-common-dir; do
    admin_raw="$(guard_git -C "${probe}" rev-parse "--${admin_kind}" 2>/dev/null)" || continue
    [ -n "${admin_raw}" ] || continue
    admin_phys="$(physical_path "${admin_raw}")" || continue
    if path_relative_inside "${admin_phys}" "${dataset_abs}" >/dev/null; then
      refuse_structural_dataset_root "'${dataset_abs}' is at or beneath git's ${admin_kind} '${admin_phys}' — deleting it would corrupt the repository and destroy the object store this guard restores FROM"
      return 0
    fi
  done

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

  TRACKED_GUARD_LIST="$(mktemp "${TRACKED_GUARD_STATE_DIR}/cqlite-tracked-datasets.XXXXXX")"
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

  # STRUCTURAL: index flags that make a path invisible to the restore and/or to the
  # integrity check. Both halves of the guard break:
  #   * `git restore` REFUSES a skip-worktree pathspec ("did not match any file(s)
  #     known to git") and fails the WHOLE batch, so up to 400 other files in that
  #     batch are not restored either;
  #   * `git diff` ignores skip-worktree AND assume-unchanged entries, so the
  #     postcondition cannot SEE their loss and would agree nothing is wrong — the
  #     foreign-index failure again: an oracle pointed at something other than the
  #     thing it verifies. `--ignore-skip-worktree-bits` would fix only the restore
  #     and leave the verification blind, so refuse, naming the remediation.
  #
  # Tag parsing, verified against git 2.43 rather than assumed — `ls-files -v`
  # LOWERCASES the tag for assume-unchanged, and the letter it lowercases is `h`,
  # not `s`: a path carrying BOTH flags reports `h`, so matching `S`/`s` misses it,
  # and `ls-files -t` reports that same path as a plain `H`, hiding it completely.
  # The invariant is therefore "every entry must be exactly `H`-class": tag `S`
  # (skip-worktree) or ANY lowercase tag (assume-unchanged, with or without
  # skip-worktree) is refused, because those are precisely the entries `git diff`
  # cannot see.
  local vlist record tag blind_count=0 blind_sample="" blind_tag=""
  if ! vlist="$(mktemp "${TRACKED_GUARD_STATE_DIR}/cqlite-tracked-flags.XXXXXX")"; then
    cleanup_tracked_guard_list
    refuse_structural_dataset_root "could not create a scratch file to check the index flags of '${rel}', so skip-worktree/assume-unchanged entries cannot be ruled out"
    return 0
  fi
  if ! guard_git -C "${repo_root_phys}" ls-files -v -z -- ":(literal)${rel}" >"${vlist}"; then
    rm -f "${vlist}"
    cleanup_tracked_guard_list
    refuse_unprotected_dataset_root "'git ls-files -v' failed for '${rel}' in '${repo_root_phys}', so skip-worktree/assume-unchanged entries cannot be ruled out"
    return 0
  fi
  while IFS= read -r -d '' record; do
    tag="${record%% *}"
    case "${tag}" in
      S | [a-z])
        blind_count=$((blind_count + 1))
        if [ -z "${blind_sample}" ]; then
          blind_sample="${record#* }"
          blind_tag="${tag}"
        fi
        ;;
    esac
  done <"${vlist}"
  rm -f "${vlist}"
  if [ "${blind_count}" -gt 0 ]; then
    cleanup_tracked_guard_list
    refuse_structural_dataset_root "${blind_count} tracked path(s) under '${rel}' carry index flags that hide them from the restore and/or the integrity check — SKIP-WORKTREE / sparse-checkout excluded (tag 'S') or ASSUME-UNCHANGED (any lowercase tag), e.g. '${blind_sample}' tagged '${blind_tag}'. 'git restore' would not rebuild a sparse-excluded path AND 'git diff' cannot see either flag class, so the loss would pass the integrity check unnoticed. Clear the flags (git update-index --no-skip-worktree --no-assume-unchanged), unsparse the path (git sparse-checkout), or point CQLITE_DATASETS_ROOT outside the affected area"
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

# Prove, BEFORE anything is deleted and in the RESTORE's exact environment, that
# every captured blob can actually be READ (issue #2878). The capture reads only
# the INDEX, so it can report a healthy count while the objects themselves are
# unreachable — an external/alternate object store, a receive-hook quarantine
# (git exports GIT_OBJECT_DIRECTORY there), a pruned or corrupt store. Deleting
# first and discovering that afterwards IS the delete-then-cannot-restore failure
# this guard exists to prevent, so it is a STRUCTURAL refusal.
#
# This makes the guard SELF-VERIFYING: the capture no longer merely claims
# recoverability, it demonstrates it under the same environment scrub the restore
# uses — which closes every future divergence between capture-env and restore-env,
# the class behind several of this change's defects. Cost is one `ls-files -s` plus
# one `cat-file --batch-check`: ~6ms over the real 875-file dataset tree.
verify_captured_blobs_readable() {
  [ "${TRACKED_GUARD_STATE}" = "in-repo" ] || return 0
  [ -n "${TRACKED_GUARD_LIST}" ] && [ -s "${TRACKED_GUARD_LIST}" ] || return 0

  local staged_list record sha path check_out line idx unreadable sample
  staged_list="$(mktemp "${TRACKED_GUARD_STATE_DIR:-${TMPDIR:-/tmp}}/cqlite-staged-blobs.XXXXXX")"
  if ! guard_git -C "${TRACKED_GUARD_REPO}" ls-files -s -z -- ":(literal)${TRACKED_GUARD_REL}" >"${staged_list}"; then
    rm -f "${staged_list}"
    refuse_unprotected_dataset_root "'git ls-files -s' failed for '${TRACKED_GUARD_REL}', so blob readability cannot be established"
    return 0
  fi

  # Records are "<mode> <sha> <stage>\t<path>". Query by SHA, not by ":path": a SHA
  # is fixed-width hex, so it streams to cat-file safely whatever the filenames
  # look like (a path may contain a newline).
  local -a shas=() paths=()
  while IFS= read -r -d '' record; do
    sha="${record#* }"
    sha="${sha%% *}"
    path="${record#*$'\t'}"
    shas+=( "${sha}" )
    paths+=( "${path}" )
  done <"${staged_list}"
  rm -f "${staged_list}"
  [ "${#shas[@]}" -gt 0 ] || return 0

  # cat-file exits 0 and prints "<sha> missing" for an unreadable object, so the
  # verdict comes from the OUTPUT, not the status; a hard failure is also fatal.
  if ! check_out="$(printf '%s\n' "${shas[@]}" \
    | guard_git -C "${TRACKED_GUARD_REPO}" cat-file --batch-check 2>&1)"; then
    refuse_structural_dataset_root "could not read the staged blobs under '${TRACKED_GUARD_REL}' ('git cat-file' failed: ${check_out}) — the object store is unreachable, so the deletion could not be undone"
    return 0
  fi

  unreadable=0
  sample=""
  idx=0
  while IFS= read -r line; do
    case "${line}" in
      *" blob "*) ;;
      *)
        unreadable=$((unreadable + 1))
        [ -n "${sample}" ] || sample="${paths[idx]:-<unknown>}"
        ;;
    esac
    idx=$((idx + 1))
  done <<EOF
${check_out}
EOF

  if [ "${unreadable}" -gt 0 ]; then
    refuse_structural_dataset_root "${unreadable} of ${#shas[@]} staged blob(s) under '${TRACKED_GUARD_REL}' are UNREADABLE in the environment the restore will use (e.g. '${sample}') — the object store is unreachable, so deleting the directory could not be undone. Nothing has been deleted"
    return 0
  fi
  return 0
}

# Enumerate the `git status --porcelain` entries that concern a TRACKED path under
# the captured subtree. Prints one "XY <path>" per line. The exit status IS the
# verdict, and a failed MEASUREMENT is deliberately not a clean reading:
#   0  measured — no tracked path under the subtree is dirty
#   2  measured — at least one is dirty; the entries are on stdout
#   1  COULD NOT MEASURE (git failed, no scratch file, unparseable record); the
#      reason is on stdout, and every caller treats it as a failure
#
# Why `git status` when restore_tracked_dataset_files already runs `git diff`
# (issue #3245): `git diff` compares the WORKTREE to the INDEX, so a path removed
# from the index but still present in HEAD — a STAGED DELETION, e.g. after
# `git rm --cached` — produces NO diff entry: there is no index entry left to
# differ from. Such a path is not captured either (`ls-files` reads the index), so
# the `rm -rf` takes it for good and the `git diff` postcondition agrees nothing is
# wrong. `git status --porcelain` compares HEAD -> index -> worktree and reports it
# as `D `. Both oracles are kept: neither is a superset of the other.
#
# Why the TRACKED subset and not raw porcelain: a fetch legitimately creates
# hundreds of untracked (and .gitignore'd) files under the dataset root, so
# asserting raw porcelain emptiness would fail every normal run.
# `--untracked-files=no` asks git not to enumerate them at all (which is also what
# keeps this cheap on a fully extracted corpus), and the explicit `??`/`!!` skip
# below is belt-and-braces for a git that reports them regardless.
tracked_dataset_dirty_entries() {
  local scratch record xy path _origin_path
  if ! scratch="$(mktemp "${TRACKED_GUARD_STATE_DIR:-${TMPDIR:-/tmp}}/cqlite-tracked-status.XXXXXX")"; then
    printf 'could not create a scratch file for the status scan\n'
    return 1
  fi
  # --no-optional-locks: `git status` otherwise rewrites the index to refresh its
  # stat cache — a needless mutation, and a lock, for a read-only oracle. git's own
  # diagnostic is left on stderr rather than swallowed.
  if ! guard_git -C "${TRACKED_GUARD_REPO}" --no-optional-locks status \
    --porcelain -z --untracked-files=no -- ":(literal)${TRACKED_GUARD_REL}" >"${scratch}"; then
    rm -f "${scratch}"
    printf "'git status --porcelain' failed for '%s' in '%s'\n" "${TRACKED_GUARD_REL}" "${TRACKED_GUARD_REPO}"
    return 1
  fi

  local -a entries=()
  while IFS= read -r -d '' record; do
    # Every porcelain v1 record is "XY <path>", so it is at least 4 characters. A
    # shorter one means this is not the format being parsed; refusing to guess is
    # mandatory on a data-loss oracle.
    if [ "${#record}" -lt 4 ]; then
      rm -f "${scratch}"
      printf "unparseable 'git status --porcelain -z' record '%s' for '%s'\n" "${record}" "${TRACKED_GUARD_REL}"
      return 1
    fi
    xy="${record:0:2}"
    path="${record:3}"
    case "${xy}" in
      R? | C? | ?R | ?C)
        # A rename/copy entry carries a SECOND NUL-terminated field (the original
        # path). Consume it so it can never be misread as the next status record.
        # shellcheck disable=SC2034  # read only to advance past the field
        IFS= read -r -d '' _origin_path || _origin_path=""
        ;;
    esac
    # Quoted so `case` matches them LITERALLY — unquoted, `??` is a two-character
    # glob and would swallow every entry.
    case "${xy}" in
      '??' | '!!') continue ;;
    esac
    entries+=( "${xy} ${path}" )
  done <"${scratch}"
  rm -f "${scratch}"

  [ "${#entries[@]}" -gt 0 ] || return 0
  printf '%s\n' "${entries[@]}"
  return 2
}

# STRUCTURAL pre-destruction refusal (issue #3245): a tracked file under the
# dataset root that carries a LOCAL modification must stop the fetch.
#
# restore_tracked_dataset_files rewrites worktree content FROM THE INDEX, so the
# `rm -rf` + `restore all` pair SILENTLY REVERTS such a file: the modification
# exists only in the tree about to be deleted, so there is nothing to restore it
# FROM; the postcondition then sees a clean subtree and the run reports SUCCESS.
# These fixtures are hand-regenerated (that is how a golden is produced), so the
# loss is real and message-free — which is exactly why this is STRUCTURAL and
# CQLITE_DATASETS_ALLOW_UNPROTECTED does NOT unlock it: an escape hatch on a
# silent-data-loss guard would be reached for precisely when it must not be.
#
# ANY dirty tracked path is refused, including the index states that would in fact
# survive (a purely staged edit is restored from the index unchanged). That
# over-refusal is deliberate: separating the safe index states from the lossy ones
# (`MD`, `AM`, a staged rename, an unborn HEAD reporting every path as `A `) is a
# per-state analysis whose failure mode is SILENT loss, while over-refusing costs
# one `git commit`/`git stash` and says so.
#
# Only the DESTRUCTIVE path calls this. `--verify-only` and the warm-cache path
# never reach the `rm -rf`, and the pre-flight `restore missing-only` skips any
# file that exists, so both stay modification-safe and ungated.
refuse_modified_tracked_dataset_files() {
  # `in-repo` is the AFFIRMATIVE precondition: an out-of-repo root holds no tracked
  # files by construction, so there is nothing this oracle could witness.
  #
  # Deliberately NOT gated on TRACKED_GUARD_COUNT (issue #3245 review): that count is
  # derived from `git ls-files`, i.e. the INDEX, so staging the deletion of every
  # tracked file under the root (`git rm --cached -r <root>`) drives it to 0. A
  # count-gated guard would then read `0` as "nothing to protect" when it actually
  # means "every tracked file is staged-deleted" — the state of MAXIMUM risk, since
  # the on-disk content is the only copy the restore cannot rebuild from the index.
  # `git status --porcelain` reports those as `D ` records and IS able to see them,
  # so the status scan is the sole authority here and always runs.
  [ "${TRACKED_GUARD_STATE}" = "in-repo" ] || return 0

  local entries rc=0 count sample
  entries="$(tracked_dataset_dirty_entries)" || rc=$?
  case "${rc}" in
    0) return 0 ;;
    2)
      count="$(printf '%s\n' "${entries}" | wc -l | tr -d ' ')"
      sample="$(printf '%s\n' "${entries}" | head -3 | tr '\n' ';' | sed 's/;$//')"
      refuse_structural_dataset_root "${count} tracked file(s) under '${TRACKED_GUARD_REL}' carry LOCAL MODIFICATIONS, staged or unstaged (${sample}) — this script deletes that directory and rebuilds those files from the git INDEX, so the modification would be SILENTLY REVERTED and the run would still report success (issue #3245). Commit them, stash them (git stash push -- '${TRACKED_GUARD_REL}'), or discard them (git restore --staged --worktree -- '${TRACKED_GUARD_REL}') first. Nothing has been deleted"
      return 0
      ;;
    *)
      refuse_structural_dataset_root "could not determine whether the tracked files under '${TRACKED_GUARD_REL}' carry local modifications (${entries}) — an unmeasured tree is not a clean one, and a local modification would be silently reverted by the delete-and-restore (issue #3245). Nothing has been deleted"
      return 0
      ;;
  esac
}

# The porcelain half of the `all`-mode postcondition (issue #3245). Returns 1 (never
# `exit`) so it is safe on the abort/trap path, like its caller. See
# tracked_dataset_dirty_entries for why `git diff` alone cannot see a staged
# deletion of a tracked fixture.
verify_tracked_status_clean_or_fail() {
  # Not gated on TRACKED_GUARD_COUNT, for the same reason as
  # refuse_modified_tracked_dataset_files above: the count is index-derived and goes
  # to 0 precisely when every tracked path under the root is staged-deleted, which is
  # the case this postcondition most needs to catch (issue #3245 review).
  [ "${TRACKED_GUARD_STATE}" = "in-repo" ] || return 0

  local entries rc=0
  entries="$(tracked_dataset_dirty_entries)" || rc=$?
  case "${rc}" in
    0) return 0 ;;
    2)
      echo "ERROR: dataset fetch left git-tracked paths under ${TRACKED_GUARD_REL} modified or DELETED as reported by 'git status --porcelain' (issue #3245; 'git diff' alone cannot see a staged deletion):" >&2
      printf '%s\n' "${entries}" | head -20 >&2
      echo "ERROR: recover with: git -C '${TRACKED_GUARD_REPO:-.}' restore --staged --worktree -- '${TRACKED_GUARD_REL:-test-data/datasets}'" >&2
      return 1
      ;;
    *)
      echo "ERROR: could not verify tracked-path status under ${TRACKED_GUARD_REL} with 'git status --porcelain' (issue #3245): ${entries}" >&2
      return 1
      ;;
  esac
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
  guard_list_is_consistent || return 1
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
    # ...and the porcelain oracle AC3 actually asks for (issue #3245): the `git
    # diff` above is worktree-vs-index, so a STAGED DELETION of a tracked path is
    # structurally invisible to it. An addition, not a replacement.
    verify_tracked_status_clean_or_fail || return 1
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
  core_fixture="$(find -H "${DATASET_ROOT}/sstables/test_basic" -path '*simple_table-*-Data.db' -print -quit 2>/dev/null || true)"
  [ -n "${core_fixture}" ] || return 1

  local data_count index_count summary_count statistics_count
  # `-H` (roborev job 9, finding 2): a DATASET_ROOT that is ITSELF a symlink — e.g.
  # `ln -s /data/datasets <somewhere>/datasets`, the natural operator layout documented as
  # #3148's "symlink trap" — is otherwise stat'ed as a plain file and never descended, so
  # every count came back 0 and a perfectly good corpus was reported UNUSABLE. `-H`
  # follows the COMMAND-LINE symlink only (not symlinks found during traversal), which is
  # exactly the semantics wanted here. Verified: without it, `--verify-only` on a
  # symlinked root failed; with it, it reports the real 155.
  data_count="$(find -H "${DATASET_ROOT}" -name '*-Data.db' 2>/dev/null | wc -l | tr -d ' ')"
  index_count="$(find -H "${DATASET_ROOT}" -name '*-Index.db' 2>/dev/null | wc -l | tr -d ' ')"
  summary_count="$(find -H "${DATASET_ROOT}" -name '*-Summary.db' 2>/dev/null | wc -l | tr -d ' ')"
  statistics_count="$(find -H "${DATASET_ROOT}" -name '*-Statistics.db' 2>/dev/null | wc -l | tr -d ' ')"

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
# #3131 deliberately did NOT touch the `rm -rf "${DATASET_ROOT}"` / the
# `[ -n "${CI:-}" ] || return 0` short-circuit in the then-named
# restore_ci_tracked_dataset_files, deferring that sibling defect to #2878. #2878 has
# since landed: that CI-only short-circuit is gone, the helper is now
# capture_tracked_dataset_files + restore_tracked_dataset_files (guarded, crash-safe,
# and verified by a `git diff` postcondition), and the pre-flight `missing-only`
# restore runs below. This function's contract is unchanged by that — it still only
# REPORTS on the resolved root and mutates nothing.
guarantee_usable_root() {
  local phase="$1"

  if ! has_required_content; then
    echo "ERROR: ${DATASET_ROOT} does not hold a usable dataset corpus (${phase})" >&2
    echo "ERROR: required content missing — expected at least metadata.yml, a" >&2
    echo "ERROR: test_basic/simple_table-*-Data.db, and the promoted wide_partition" >&2
    echo "ERROR: reference binaries under" >&2
    echo "ERROR:   ${WIDE_PARTITION_DIR}" >&2
    echo "ERROR: remedy: re-run this script with the pin cleared so it re-downloads:" >&2
    # %q on both interpolations (roborev job 11, nit 3), for the same reason as the export line
    # below: an unquoted path containing a space or a shell metacharacter prints a command that
    # BREAKS (or does something else) when pasted, so the "remedy" would not be one.
    # shellcheck disable=SC2059  # %q is the point; the values are arguments, not the format
    printf 'ERROR:   rm -f %q && CQLITE_DATASETS_ROOT=%q bash test-data/scripts/fetch-datasets.sh\n' \
      "${PIN_FILE}" "${DATASET_ROOT}" >&2
    exit 1
  fi

  local data_count
  # -H: see has_required_content — a symlinked DATASET_ROOT must not report 0.
  data_count="$(find -H "${DATASET_ROOT}" -name '*-Data.db' 2>/dev/null | wc -l | tr -d ' ')"

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

# ---------------------------------------------------------------------------
# --verify-only tracked-fixture probe (issue #3310) — REPORT ONLY, NEVER REPAIR.
#
# The #2878 crash-safe restore covers EXIT/INT/TERM/HUP. SIGKILL outruns all of
# them, so a killed fetch can leave git-tracked fixtures DELETED on disk with the
# index still recording them. Recovery does exist — the DESTRUCTIVE path's
# `restore_tracked_dataset_files missing-only` pre-flight — but `--verify-only`
# exits above it, so the documented "is my root usable?" probe answered either a
# clean green (corpus intact, fixtures gone) or the generic "does not hold a usable
# dataset corpus" (corpus gone too), and neither names the actual damage or its
# one-line repair. The first diagnostic an agent reaches for therefore misled.
#
# DESIGN DECISION, recorded in #3310 and enforced here structurally: this probe
# REPORTS, it does not repair. `--verify-only` promises to mutate nothing (#3131
# blocker B2), and a probe that quietly fixed the tree would be a second,
# unannounced destructive-adjacent path in the one mode operators trust to be
# inert. It therefore names the files, prints the exact repair command, and exits
# non-zero; the repair is the operator's (or the next real fetch's) to run.
# ---------------------------------------------------------------------------

# Locate the (repository, subtree) pair a tracked census would apply to, using the
# SAME git plumbing as capture_tracked_dataset_files but with NONE of its
# refusals: every refuse_* there is a statement about a `rm -rf` that this mode
# never performs, and printing "refusing to replace ..." from a read-only probe
# would be false. Sets TRACKED_GUARD_REPO/TRACKED_GUARD_REL on success.
#
#   0  in-repo — REPO/REL are valid
#   1  provably OUTSIDE any git work tree; the probe has NO SUBJECT
#   2  COULD NOT MEASURE; the reason is in TRACKED_PROBE_REASON
#
# The reason travels in a GLOBAL rather than on stdout because this function
# PUBLISHES globals: `reason="$(resolve_tracked_subtree_readonly)"` would run it in
# a subshell, and TRACKED_GUARD_REPO/REL would be discarded with it — an empty REL
# then makes the `:(literal)` pathspec match the WHOLE repository, so the probe
# would census (and report on) every tracked file in the checkout.
#
# The `1` verdict is an affirmative proof (git says there is no work tree, or —
# when git is absent — no ancestor carries a `.git`), never a fallback for an
# unanswered question: that distinction is the whole point of the `2` verdict.
TRACKED_PROBE_REASON=""
resolve_tracked_subtree_readonly() {
  local dataset_abs probe repo_root repo_root_phys rel
  TRACKED_PROBE_REASON=""
  TRACKED_GUARD_REPO=""
  TRACKED_GUARD_REL=""

  if ! dataset_abs="$(physical_path "${DATASET_ROOT}")"; then
    TRACKED_PROBE_REASON="could not resolve a physical path for '${DATASET_ROOT}'"
    return 2
  fi

  if ! command -v git >/dev/null 2>&1; then
    if ancestor_has_git_dir "${dataset_abs}"; then
      TRACKED_PROBE_REASON="git is not installed, but '${dataset_abs}' is inside a git checkout, so its tracked files cannot be enumerated"
      return 2
    fi
    return 1
  fi

  probe="$(nearest_existing_dir "${dataset_abs}")"
  if ! repo_root="$(guard_git -C "${probe}" rev-parse --show-toplevel 2>/dev/null)" || [ -z "${repo_root}" ]; then
    if ancestor_has_git_dir "${dataset_abs}"; then
      TRACKED_PROBE_REASON="'${dataset_abs}' looks like it is inside a git checkout, but 'git -C ${probe} rev-parse --show-toplevel' reported no work tree"
      return 2
    fi
    return 1
  fi

  if ! repo_root_phys="$(physical_path "${repo_root}")"; then
    TRACKED_PROBE_REASON="could not resolve a physical path for the enclosing repository '${repo_root}'"
    return 2
  fi

  if ! rel="$(path_relative_inside "${repo_root_phys}" "${dataset_abs}")"; then
    TRACKED_PROBE_REASON="could not decide whether '${dataset_abs}' is inside the git work tree at '${repo_root_phys}' (probed from '${probe}')"
    return 2
  fi

  if [ "${rel}" = "." ]; then
    TRACKED_PROBE_REASON="'${dataset_abs}' IS the root of the git work tree at '${repo_root_phys}', so a tracked census there would cover the whole repository rather than a fixture subtree"
    return 2
  fi

  TRACKED_GUARD_REPO="${repo_root_phys}"
  TRACKED_GUARD_REL="${rel}"
  return 0
}

# The COULD-NOT-MEASURE verdict, textually distinct from both the missing-fixture
# report and guarantee_usable_root's "does not hold a usable dataset corpus".
report_unmeasurable_tracked_census() {
  echo "ERROR: TRACKED-FIXTURE PROBE COULD NOT MEASURE '${DATASET_ROOT}' (issue #3310): $1" >&2
  echo "ERROR: an unmeasured tracked-fixture census is NOT a clean one, so --verify-only" >&2
  echo "ERROR: cannot report this root usable. Nothing was created, deleted or restored." >&2
}

# The probe itself. Returns 1 when it must fail the run (fixtures missing, or the
# census could not be taken), 0 otherwise. Writes only outside the dataset root.
report_orphaned_tracked_fixtures() {
  local resolve_rc=0 dataset_abs census_file tracked_count=0 rel_path
  local scan_rc=0 entries record xy path staged_deletion=0 repair_flags shown=0
  local -a deleted=()

  resolve_tracked_subtree_readonly || resolve_rc=$?
  case "${resolve_rc}" in
    0) ;;
    1)
      # No subject — and said as such. A "0 of 0 clean" verdict here would be a
      # positive claim about a census that has no subject to take (#3310).
      echo "Tracked-fixture probe (#3310): NO SUBJECT — '${DATASET_ROOT}' lies outside any git work tree, so no git-tracked fixture can be missing from it"
      return 0
      ;;
    *)
      report_unmeasurable_tracked_census "${TRACKED_PROBE_REASON}"
      return 1
      ;;
  esac

  # The status scan needs a scratch file, and --verify-only must not write inside
  # the root it is probing — resolve_guard_state_dir PROVES a location outside it.
  if ! dataset_abs="$(physical_path "${DATASET_ROOT}")"; then
    report_unmeasurable_tracked_census "could not resolve a physical path for '${DATASET_ROOT}'"
    return 1
  fi
  if ! TRACKED_GUARD_STATE_DIR="$(resolve_guard_state_dir "${dataset_abs}")"; then
    report_unmeasurable_tracked_census "no writable temporary directory (of TMPDIR, /tmp, HOME) could be PROVEN to lie outside '${dataset_abs}', so the census could not be taken without writing inside the root this probe promises not to touch"
    return 1
  fi

  # (1) The INDEX census — how many tracked files the subtree is supposed to hold.
  # Used ONLY to distinguish "no subject" from "all present"; it deliberately does
  # NOT gate the status scan below, because `git ls-files` reads the index and so
  # reports 0 precisely when every tracked path is staged-deleted — the state of
  # maximum risk (the #3245 review blocker, in a new place).
  if ! census_file="$(mktemp "${TRACKED_GUARD_STATE_DIR}/cqlite-probe-census.XXXXXX")"; then
    report_unmeasurable_tracked_census "could not create a scratch file for the tracked-file census"
    return 1
  fi
  if ! guard_git -C "${TRACKED_GUARD_REPO}" ls-files -z -- ":(literal)${TRACKED_GUARD_REL}" >"${census_file}"; then
    rm -f "${census_file}"
    report_unmeasurable_tracked_census "'git ls-files' failed for '${TRACKED_GUARD_REL}' in '${TRACKED_GUARD_REPO}'"
    return 1
  fi
  # NUL-delimited (`-z`) throughout: this repo tracks 40 space-bearing paths, and a
  # whitespace-split census would silently miscount or mangle them.
  while IFS= read -r -d '' rel_path; do
    tracked_count=$((tracked_count + 1))
  done <"${census_file}"
  rm -f "${census_file}"

  # (2) The authority: the same porcelain oracle the #3245 guards use. Its `-z`
  # git read is what makes space-bearing paths safe; its OUTPUT is one "XY <path>"
  # per line, which is the helper's established contract (shared with
  # refuse_modified_tracked_dataset_files and verify_tracked_status_clean_or_fail).
  entries="$(tracked_dataset_dirty_entries)" || scan_rc=$?
  case "${scan_rc}" in
    0 | 2) ;;
    *)
      report_unmeasurable_tracked_census "${entries}"
      return 1
      ;;
  esac

  if [ "${scan_rc}" = 2 ]; then
    while IFS= read -r record; do
      [ -n "${record}" ] || continue
      xy="${record:0:2}"
      path="${record:3}"
      case "${xy}" in
        # Either half of the status pair reporting a deletion: ' D' (deleted in the
        # worktree, the SIGKILL shape), 'D ' (staged deletion), 'DD'/'AD'/'MD'/'RD'.
        D? | ?D) deleted+=( "${path}" ) ;;
        *) continue ;;
      esac
      case "${xy}" in
        D?) staged_deletion=1 ;;
      esac
    done <<EOF
${entries}
EOF
  fi

  if [ "${#deleted[@]}" -gt 0 ]; then
    # `git restore --worktree` rebuilds a worktree deletion from the index; a
    # STAGED deletion has no index entry left, so that spelling would silently do
    # nothing and `--staged` is required. The printed line is chosen by
    # MEASUREMENT rather than printing the broader form unconditionally, which
    # would revert unrelated staged work under the same path.
    repair_flags="--worktree"
    [ "${staged_deletion}" = 1 ] && repair_flags="--staged --worktree"

    echo "ERROR: TRACKED FIXTURES MISSING under '${TRACKED_GUARD_REL}' (issue #3310): ${#deleted[@]} git-tracked" >&2
    echo "ERROR: file(s) recorded in git are DELETED on disk. This is NOT the 'does not hold a" >&2
    echo "ERROR: usable dataset corpus' condition — the fetched corpus may be perfectly fine." >&2
    echo "ERROR: A fetch killed with SIGKILL outruns the #2878 EXIT/INT/TERM/HUP restore and" >&2
    echo "ERROR: leaves exactly this state." >&2
    echo "ERROR: missing tracked file(s):" >&2
    for path in "${deleted[@]}"; do
      shown=$((shown + 1))
      [ "${shown}" -le 20 ] || break
      echo "ERROR:   ${path}" >&2
    done
    if [ "${#deleted[@]}" -gt 20 ]; then
      echo "ERROR:   … and $(( ${#deleted[@]} - 20 )) more" >&2
    fi
    echo "ERROR: --verify-only REPORTS ONLY (issue #3310): nothing was created, deleted or" >&2
    echo "ERROR: restored. Repair with EXACTLY this command:" >&2
    # %q for the same reason as guarantee_usable_root's remedy line: a path holding
    # a space or a shell metacharacter must still paste as a working command.
    # shellcheck disable=SC2059  # %q is the point; the values are arguments, not the format
    printf 'ERROR:   git -C %q restore %s -- %q\n' \
      "${TRACKED_GUARD_REPO}" "${repair_flags}" "${TRACKED_GUARD_REL}" >&2
    return 1
  fi

  if [ "${tracked_count}" -eq 0 ]; then
    echo "Tracked-fixture probe (#3310): NO SUBJECT — the git work tree at '${TRACKED_GUARD_REPO}' tracks no file under '${TRACKED_GUARD_REL}', so none can be missing"
    return 0
  fi

  echo "Tracked-fixture probe (#3310): OK — all ${tracked_count} git-tracked file(s) under '${TRACKED_GUARD_REL}' are present on disk"
  return 0
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
  # BEFORE guarantee_usable_root, deliberately (issue #3310): with the corpus also
  # gone, the content check would exit first and answer "root unusable", sending an
  # operator to a re-fetch instead of to the one-line restore. The more specific
  # diagnosis has to be the one that speaks.
  report_orphaned_tracked_fixtures || exit 1
  guarantee_usable_root "verify-only (no download, no extraction)"
  exit 0
fi

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
    # DISCARD any partial extraction output first. The live extraction path stages
    # into a temp dir and `mv`s into place, and that `mv` is NOT atomic when TMPDIR
    # is on a different filesystem (the usual /tmp-is-tmpfs case): a copy
    # interrupted mid-way leaves a partially-populated dataset tree. Removing it
    # here gives a single well-defined post-abort state — no archive content, all
    # tracked fixtures restored — and makes the message below a VERIFIED statement
    # rather than an assumption.
    rm -rf "${DATASET_ROOT}" 2>/dev/null || true
    # `all`, not `missing-only`: an abort after the extraction landed can leave a
    # tracked reference file present but overwritten by the archive's stale copy,
    # and this mode also VERIFIES the subtree ends clean.
    if restore_tracked_dataset_files all; then
      echo "WARNING: git-tracked fixtures under ${TRACKED_GUARD_REL:-${DATASET_ROOT}} restored; any partial extraction output was discarded, so the archive content is NOT present — re-run the fetch" >&2
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

# Re-capture immediately before the destructive step so the restored list is
# exactly what existed at deletion time (issue #2878 AC1: capture, never
# re-derive after the fact). capture_* exits rather than returning when it cannot
# determine whether tracked files are at risk.
capture_tracked_dataset_files

# ...and prove every captured blob is READABLE in the restore's own environment
# before the deletion, rather than discovering it afterwards.
verify_captured_blobs_readable

# ...and refuse outright if any tracked fixture carries a LOCAL modification: the
# restore rebuilds from the INDEX, so the delete-and-restore would silently REVERT
# it and still report success (issue #3245). Placed here — after the re-capture,
# immediately before the destructive window opens — so the verdict is about the
# tree state at deletion time; the warm-cache and --verify-only paths exit above
# and are unaffected.
refuse_modified_tracked_dataset_files

# From here until the restore below verifies the tracked subtree clean, an abort
# on ANY path must restore on the way out — see cleanup_fetch_temporaries.
TRACKED_GUARD_DESTRUCTIVE_STARTED=1

rm -rf "${DATASET_ROOT}"
# NOTE (#3198): this in-place `tar -C .` branch is currently UNREACHABLE —
# canonicalize_dataset_root rewrites DATASET_ROOT to an ABSOLUTE path, which can
# never equal the relative ARCHIVE_DATASET_ROOT, so the staged tmp+mv branch below
# always runs. Left as-is deliberately; resolving the dead branch belongs to #3198,
# not to #2878's data-safety work.
if [ "${DATASET_ROOT}" = "${ARCHIVE_DATASET_ROOT}" ]; then
  tar -xzf "${ASSET_PATH}" -C . --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'
else
  # Staged in the PROVEN-safe directory, never a TMPDIR that may sit inside the
  # dataset tree we just deleted (issue #2878).
  EXTRACT_TMP="$(mktemp -d "${TRACKED_GUARD_STATE_DIR:-${TMPDIR:-/tmp}}/cqlite-dataset-extract.XXXXXX")"
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
guarantee_usable_root "fresh extraction"
