#!/usr/bin/env bash
#
# Regression tests for the WARM-CACHE half of test-data/scripts/fetch-datasets.sh (#4508).
#
# CI restores the cached dataset directory with `tar -xf` over the checkout. That overwrites
# same-named git-TRACKED files with the cache's bytes, leaves cached extras behind, and the pin file
# arriving inside the cache sent fetch-datasets.sh down its warm fast path, which only repaired
# MISSING tracked files: a PR that changed tracked bytes was tested against the old bundle bytes.
#
# HERMETIC: a mktemp sandbox holding a throwaway `git init` repo, a locally built archive and a stub
# `curl`; the REAL fetch-datasets.sh runs. The "cache overlay" is simulated exactly as CI does it, by
# extracting the cold run's bundle copy over the tracked file.
#
# Run standalone:   bash scripts/tests/test_fetch_datasets_warm_overlay.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
FETCH="${FETCH_SCRIPT:-$REPO_ROOT/test-data/scripts/fetch-datasets.sh}"   # FETCH_SCRIPT: mutation checks
PASS=0; FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
[ -f "$FETCH" ] || { echo "FAIL - missing $FETCH"; exit 1; }
command -v git >/dev/null 2>&1 || { echo "FAIL - git is required"; exit 1; }
T=$(mktemp -d "${TMPDIR:-/tmp}/fetch-warm-overlay.XXXXXX") || { echo "FAIL - mktemp -d failed"; exit 1; }
case "$T" in /?*) ;; *) echo "FAIL - sandbox path '$T' is not absolute"; exit 1 ;; esac
ASSET="cqlite-4508-fake-$$.tar.gz"
trap 'rm -rf "$T" "/tmp/$ASSET"' EXIT

# --- the fake bundle: carries OLD copies of tracked files (the overlay) and untracked binaries ----
WIDE="sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
AD="$T/archive-src/test-data/datasets"
mkdir -p "$AD/sstables/test_basic/simple_table-aaaa" "$AD/$WIDE" "$AD/goldens"
printf 'meta\n' > "$AD/metadata.yml"
for s in Data.db Index.db Summary.db Statistics.db; do printf 'a %s\n' "$s" > "$AD/sstables/test_basic/simple_table-aaaa/nb-1-big-$s"; done
for s in Data.db Index.db Digest.crc32 CompressionInfo.db; do printf 'w %s\n' "$s" > "$AD/$WIDE/nb-2-big-$s"; done
printf '{"wide":1}\n' > "$AD/$WIDE/nb-2-big-Data.db.jsonl"
printf 'OLD-BUNDLE-BYTES\n' > "$AD/goldens/tracked-Data.db.jsonl"            # tracked in the repo with NEW bytes
printf 'removed upstream\n' > "$AD/goldens/removed-Data.db.jsonl"             # tracked at build; the PR deletes it
printf 'old name\n' > "$AD/goldens/old-name-Data.db.jsonl"                    # tracked at build; the PR renames it
printf 'legit binary\n' > "$AD/goldens/bundle-only.db"                        # untracked bundle member: must survive
TARBALL="$T/$ASSET"; tar -czf "$TARBALL" -C "$T/archive-src" test-data
sha_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
SHA="$(sha_of "$TARBALL")"

BIN="$T/bin"; mkdir -p "$BIN"
printf '#!/usr/bin/env bash\nout=""\nwhile [ "$#" -gt 0 ]; do case "$1" in -o) out="$2"; shift 2;; *) shift;; esac; done\ncp "$STUB_CURL_PAYLOAD" "$out"\n' > "$BIN/curl"
chmod +x "$BIN/curl"

NEW='NEW-TRACKED-BYTES (this PR)'
DS="test-data/datasets"
make_repo() {   # the tree the cache was BUILT from: tracks tracked/removed/old-name
  local d="$1"; mkdir -p "$d/$DS/goldens"
  git -C "$d" init -q; git -C "$d" config user.email t@example.com; git -C "$d" config user.name T
  printf '*.db\n%s/metadata.yml\n%s/sstables/\n%s/.dataset-*\n' "$DS" "$DS" "$DS" > "$d/.gitignore"
  printf '%s\n' "$NEW" > "$d/$DS/goldens/tracked-Data.db.jsonl"
  printf 'removed upstream\n' > "$d/$DS/goldens/removed-Data.db.jsonl"
  printf 'old name\n' > "$d/$DS/goldens/old-name-Data.db.jsonl"
  git -C "$d" add .gitignore; git -C "$d" add -f "$DS/goldens"
  git -C "$d" commit -qm built
}
pr_changes() {  # the PR: delete one tracked file, rename another
  git -C "$1" rm -q "$DS/goldens/removed-Data.db.jsonl"
  git -C "$1" mv "$DS/goldens/old-name-Data.db.jsonl" "$DS/goldens/new-name-Data.db.jsonl"
  git -C "$1" commit -qm "PR: delete + rename"
}
# run_fetch <repo> [env assignments...] -> $OUT $RC   (CI is UNSET unless passed)
run_fetch() {
  local repo="$1"; shift
  OUT=$( cd "$repo" || exit 90
         unset CI GITHUB_ACTIONS CQLITE_DATASETS_ALLOW_UNPROTECTED CQLITE_DATASETS_RESTORE_TRACKED CQLITE_DATASETS_ROOT
         export PATH="$BIN:$PATH" TMPDIR="$T" STUB_CURL_PAYLOAD="$TARBALL"
         env "$@" DATASET_TAG=fake-tag DATASET_ASSET="$ASSET" DATASET_SHA256="$SHA" bash "$FETCH" 2>&1 )
  RC=$?
}
overlay_cache() {   # what actions/cache does: extract the cached tree over the checkout
  tar -xzf "$TARBALL" -C "$1" test-data/datasets/goldens/tracked-Data.db.jsonl test-data/datasets/goldens/removed-Data.db.jsonl test-data/datasets/goldens/old-name-Data.db.jsonl
}
G="$DS/goldens"
has() { [ -e "$1/$G/$2" ]; }
tracked_is() { [ "$(cat "$1/$G/tracked-Data.db.jsonl")" = "$2" ]; }
reset_extra() { printf 'stale\n' > "$1/$G/stale-leftover.log"; }

R="$T/r"; make_repo "$R"

# 1. cold run on the BUILD tree: bundle extracted, tracked bytes win, both build-time lists written
run_fetch "$R"
[ "$RC" = 0 ] && ok "cold run succeeds" || bad "cold run failed ($RC): $OUT"
tracked_is "$R" "$NEW" && ok "cold run: the tracked file keeps the checkout's bytes over the bundle's" || bad "cold run: tracked bytes lost"
M="$R/$DS/.dataset-manifest"; B="$R/$DS/.dataset-tracked-at-build"
{ [ -s "$M" ] && grep -qx 'goldens/bundle-only.db' "$M" && grep -qx 'goldens/old-name-Data.db.jsonl' "$M"; } \
  && ok "cold run writes .dataset-manifest listing the bundle's members" || bad "no usable .dataset-manifest"
{ grep -qx 'goldens/removed-Data.db.jsonl' "$B" && grep -qx 'goldens/old-name-Data.db.jsonl' "$B" && ! grep -qx 'goldens/bundle-only.db' "$B"; } \
  && ok "cold run writes .dataset-tracked-at-build: the tracked-class files, not the untracked binary" || bad "wrong .dataset-tracked-at-build: $(cat "$B")"

# the PR now deletes one tracked file and renames another; CI's cache is the build-time tree
pr_changes "$R"

# 2. warm overlay in CI
overlay_cache "$R"; reset_extra "$R"
tracked_is "$R" "OLD-BUNDLE-BYTES" && has "$R" removed-Data.db.jsonl && has "$R" old-name-Data.db.jsonl \
  && ok "setup: the overlay replaced the tracked file and brought the deleted/renamed files back as untracked twins (the #4508/#4493 state)" || bad "setup: overlay did not apply"
run_fetch "$R" CI=true
[ "$RC" = 0 ] && ok "warm CI run succeeds" || bad "warm CI run failed ($RC): $OUT"
case "$OUT" in *"already present"*) ok "warm CI run took the fast path (no download)" ;; *) bad "not the warm path: $OUT" ;; esac
tracked_is "$R" "$NEW" && ok "warm CI run RESTORES the overlaid tracked file" || bad "warm CI run left the overlaid bytes in place"
{ ! has "$R" removed-Data.db.jsonl && ! has "$R" old-name-Data.db.jsonl; } && ok "deleted-tracked and renamed-tracked twins are REMOVED" || bad "a stale twin survived"
has "$R" new-name-Data.db.jsonl && ok "the renamed file's new name is kept" || bad "the new name vanished"
case "$OUT" in *"REMOVED stale twin"*"removed-Data.db.jsonl"*"REMOVED stale twin"*"old-name-Data.db.jsonl"*|*"REMOVED stale twin"*"old-name-Data.db.jsonl"*"REMOVED stale twin"*"removed-Data.db.jsonl"*) ok "each twin removal is logged by name" ;; *) bad "twin removals not logged: $OUT" ;; esac
! has "$R" stale-leftover.log && case "$OUT" in *"REMOVED stale extra"*"stale-leftover.log"*) ok "an extra in neither git nor the bundle is removed and logged" ;; *) bad "extra not logged: $OUT" ;; esac || bad "the extra survived"
has "$R" bundle-only.db && ok "an untracked bundle member (a binary) is never touched" || bad "a legitimate bundle binary was removed"
[ -z "$(git -C "$R" diff --name-only -- "$DS")" ] && ok "git diff over the dataset root is clean" || bad "tracked drift remains"

# 3. warm overlay locally: report only, nothing touched
overlay_cache "$R"; reset_extra "$R"
run_fetch "$R"
{ [ "$RC" = 0 ] && tracked_is "$R" "OLD-BUNDLE-BYTES" && has "$R" removed-Data.db.jsonl && has "$R" old-name-Data.db.jsonl && has "$R" stale-leftover.log; } \
  && ok "local warm run touches nothing" || bad "local warm run changed files or failed ($RC)"
case "$OUT" in *"differ from the index"*"tracked-Data.db.jsonl"*) ok "local run names the drifted tracked file" ;; *) bad "drift not reported: $OUT" ;; esac
case "$OUT" in *"no longer tracked but still on disk"*"removed-Data.db.jsonl"*) ok "local run names the stale twins" ;; *) bad "twins not reported: $OUT" ;; esac
case "$OUT" in *"stale-leftover.log"*) ok "local run names the stale extra" ;; *) bad "extra not reported: $OUT" ;; esac

# 4. explicit opt-in cleans up locally
run_fetch "$R" CQLITE_DATASETS_RESTORE_TRACKED=1
{ [ "$RC" = 0 ] && tracked_is "$R" "$NEW" && ! has "$R" removed-Data.db.jsonl && ! has "$R" old-name-Data.db.jsonl && ! has "$R" stale-leftover.log; } \
  && ok "CQLITE_DATASETS_RESTORE_TRACKED=1 restores, removes twins and extras locally" || bad "opt-in cleanup failed ($RC)"

# 5. a cache from before #4508 (no lists): CI refuses to trust it and goes cold; locally it is only noted
overlay_cache "$R"
rm -f "$R/$DS/.dataset-manifest" "$R/$DS/.dataset-tracked-at-build"
run_fetch "$R"
{ [ "$RC" = 0 ] && case "$OUT" in *"cannot be classified"*) true ;; *) false ;; esac; } && ok "pre-#4508 cache, local: noted, not guessed" || bad "pre-manifest local run: $OUT"
git -C "$R" checkout -q -- "$DS" 2>/dev/null
run_fetch "$R" CI=true
case "$OUT" in *"forcing a COLD restore"*"Fetching dataset"*) ok "pre-#4508 cache, CI: refuses the warm path and runs the cold restore" ;; *) bad "no cold restore: $OUT" ;; esac
{ [ "$RC" = 0 ] && tracked_is "$R" "$NEW" && [ -s "$R/$DS/.dataset-manifest" ] && [ -f "$R/$DS/.dataset-tracked-at-build" ]; } \
  && ok "after the forced cold restore the tracked bytes are the checkout's and both lists exist" || bad "cold restore left a bad state ($RC)"

# 6. a change that deletes EVERY tracked dataset file (roborev job 195): the tracked list is the empty set,
#    not a failure, and the cached twins are still removed
R2="$T/r2"; make_repo "$R2"
run_fetch "$R2"
git -C "$R2" rm -rq "$G"; git -C "$R2" commit -qm "PR: delete every tracked dataset file"
[ -z "$(git -C "$R2" ls-files -- "$DS")" ] && ok "setup: no tracked file is left under the dataset root" || bad "setup: files still tracked"
overlay_cache "$R2"; reset_extra "$R2"
run_fetch "$R2" CI=true
{ [ "$RC" = 0 ] && ! has "$R2" tracked-Data.db.jsonl && ! has "$R2" removed-Data.db.jsonl && ! has "$R2" old-name-Data.db.jsonl && ! has "$R2" stale-leftover.log; } \
  && ok "all tracked files deleted: every cached twin and the extra are removed, exit 0" || bad "all-deleted case left stale files or failed ($RC): $OUT"
has "$R2" bundle-only.db && ok "all tracked files deleted: the untracked binary is kept" || bad "the binary was removed"

# 7. an unexpected classification failure propagates as a FAILURE in CI (not a silent warm success)
classify() {   # <state> -> $OUT $RC from the sourced library with a fabricated guard state
  OUT=$( DATASET_ROOT="$T/r/$DS" ARCHIVE_DATASET_ROOT=test-data/datasets TRACKED_GUARD_STATE="$1" TRACKED_GUARD_LIST="${2:-}" TRACKED_GUARD_REL="$DS" \
         CI=1 bash -c 'set -e; . "$0"; warm_cache_reconcile; echo RECONCILED' "$REPO_ROOT/test-data/scripts/lib/dataset-warm-restore.sh" 2>&1 )
  RC=$?
}
printf 'x\n' > "$T/r/$DS/.dataset-manifest"; : > "$T/r/$DS/.dataset-tracked-at-build"
classify unprotected
{ [ "$RC" = 1 ] && case "$OUT" in *"could not classify"*) true ;; *) false ;; esac; } && ok "an unprotected guard state fails the CI reconcile (rc 1, named)" || bad "unprotected state did not fail: rc=$RC $OUT"
classify in-repo "$T/does-not-exist"
{ [ "$RC" = 1 ] && case "$OUT" in *"could not classify"*) true ;; *) false ;; esac; } && ok "a missing tracked list fails the CI reconcile" || bad "missing list did not fail: rc=$RC $OUT"
classify out-of-repo
{ [ "$RC" = 0 ] && case "$OUT" in *RECONCILED*) true ;; *) false ;; esac; } && ok "a dataset root outside any git repo has nothing to classify and succeeds" || bad "out-of-repo case: rc=$RC $OUT"

# 8. the cache key carries the format version everywhere it is composed from the pin (roborev job 195): a cache built
#    before the metadata existed must never be an exact hit, or it would force a cold download on every job forever.
#    (coverage.yml keys a literal sha and downloads the release itself without fetch-datasets.sh: out of scope.)
FMT="$(sed -n 's/^DATASET_CACHE_FORMAT=//p' "$REPO_ROOT/test-data/scripts/lib/dataset-warm-restore.sh")"
bad_keys=""; good=0
for f in "$REPO_ROOT"/.github/workflows/*.yml "$REPO_ROOT"/.github/actions/*/action.yml; do
  while IFS= read -r line; do
    case "$line" in
      *"datasets-${FMT}-"*) good=$((good + 1)) ;;
      *) bad_keys="$bad_keys $(basename "$f"): ${line#"${line%%[![:space:]]*}"};" ;;
    esac
  done < <(grep -E '(^[[:space:]]*key:.*datasets-(fmt[0-9]+-)?[$]\{\{|^[[:space:]]+datasets-(fmt[0-9]+-)?[$]\{\{)' "$f" || true)
done
[ -z "$bad_keys" ] && [ "$good" -ge 15 ] && ok "every dataset cache key and restore-key carries the format segment datasets-${FMT}- ($good sites)" || bad "dataset cache keys without datasets-${FMT}-:$bad_keys (carrying: $good)"
case "$FMT" in fmt[0-9]*) ok "the format constant has the fmtN shape" ;; *) bad "unexpected DATASET_CACHE_FORMAT '$FMT'" ;; esac

echo "test_fetch_datasets_warm_overlay: pass=$PASS fail=$FAIL"
[ "$FAIL" = 0 ]
