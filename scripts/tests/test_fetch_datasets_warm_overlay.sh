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

# --- the fake bundle: carries an OLD copy of one tracked file (the overlay) ------------------
WIDE="sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
AD="$T/archive-src/test-data/datasets"
mkdir -p "$AD/sstables/test_basic/simple_table-aaaa" "$AD/$WIDE" "$AD/goldens"
printf 'meta\n' > "$AD/metadata.yml"
for s in Data.db Index.db Summary.db Statistics.db; do printf 'a %s\n' "$s" > "$AD/sstables/test_basic/simple_table-aaaa/nb-1-big-$s"; done
for s in Data.db Index.db Digest.crc32 CompressionInfo.db; do printf 'w %s\n' "$s" > "$AD/$WIDE/nb-2-big-$s"; done
printf '{"wide":1}\n' > "$AD/$WIDE/nb-2-big-Data.db.jsonl"
printf 'OLD-BUNDLE-BYTES\n' > "$AD/goldens/tracked-Data.db.jsonl"        # also tracked in the repo, NEW bytes there
TARBALL="$T/$ASSET"; tar -czf "$TARBALL" -C "$T/archive-src" test-data
sha_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
SHA="$(sha_of "$TARBALL")"

BIN="$T/bin"; mkdir -p "$BIN"
printf '#!/usr/bin/env bash\nout=""\nwhile [ "$#" -gt 0 ]; do case "$1" in -o) out="$2"; shift 2;; *) shift;; esac; done\ncp "$STUB_CURL_PAYLOAD" "$out"\n' > "$BIN/curl"
chmod +x "$BIN/curl"

NEW='NEW-TRACKED-BYTES (this PR)'
make_repo() {
  local d="$1"; mkdir -p "$d/test-data/datasets/goldens"
  git -C "$d" init -q; git -C "$d" config user.email t@example.com; git -C "$d" config user.name T
  printf '*.db\ntest-data/datasets/metadata.yml\ntest-data/datasets/sstables/\ntest-data/datasets/.dataset-pin\ntest-data/datasets/.dataset-manifest\n' > "$d/.gitignore"
  printf '%s\n' "$NEW" > "$d/test-data/datasets/goldens/tracked-Data.db.jsonl"
  git -C "$d" add .gitignore; git -C "$d" add -f test-data/datasets/goldens/tracked-Data.db.jsonl
  git -C "$d" commit -qm tracked
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
overlay_bundle_over_checkout() {   # what actions/cache does: extract the cached tree over the checkout
  tar -xzf "$TARBALL" -C "$1" --strip-components=0 test-data/datasets/goldens/tracked-Data.db.jsonl
}
TRK="test-data/datasets/goldens/tracked-Data.db.jsonl"
content_is() { [ "$(cat "$1/$TRK")" = "$2" ]; }

# 1. cold run: bundle extracted, tracked bytes win, the manifest and pin are written
R1="$T/r1"; make_repo "$R1"
run_fetch "$R1"
[ "$RC" = 0 ] && ok "cold run succeeds" || bad "cold run failed ($RC): $OUT"
content_is "$R1" "$NEW" && ok "cold run: the tracked file keeps the checkout's bytes over the bundle's" || bad "cold run: tracked bytes lost"
[ -s "$R1/test-data/datasets/.dataset-manifest" ] && grep -qx 'goldens/tracked-Data.db.jsonl' "$R1/test-data/datasets/.dataset-manifest" \
  && ok "cold run writes .dataset-manifest listing the bundle's files" || bad "no usable .dataset-manifest"

# 2. warm overlay in CI: the tracked file is overwritten by the cache, a stale extra appears
overlay_bundle_over_checkout "$R1"
content_is "$R1" "OLD-BUNDLE-BYTES" && ok "setup: the overlay really did replace the tracked file (this is the #4508 state)" || bad "setup: overlay did not apply"
printf 'stale\n' > "$R1/test-data/datasets/goldens/stale-leftover.log"
run_fetch "$R1" CI=true
[ "$RC" = 0 ] && ok "warm CI run succeeds" || bad "warm CI run failed ($RC): $OUT"
case "$OUT" in *"already present"*) ok "warm CI run took the fast path (no download)" ;; *) bad "not the warm path: $OUT" ;; esac
content_is "$R1" "$NEW" && ok "warm CI run RESTORES the tracked file to the checkout's bytes" || bad "warm CI run left the overlaid bytes in place"
[ -z "$(git -C "$R1" diff --name-only -- test-data/datasets)" ] && ok "git diff over the dataset root is clean after the warm CI run" || bad "tracked drift remains"
case "$OUT" in *"stale-leftover.log"*) ok "the stale untracked extra is reported by name" ;; *) bad "stale extra not reported: $OUT" ;; esac
case "$OUT" in *"WARNING:   goldens/tracked"*|*"WARNING:   sstables/"*|*"WARNING:   .dataset"*) bad "bundle/tracked/pin files wrongly reported as extras: $OUT" ;; *) ok "bundle files, tracked files and the pin/manifest are not reported as extras" ;; esac

# 3. warm overlay locally (CI unset): reported, NOT touched
overlay_bundle_over_checkout "$R1"
run_fetch "$R1"
[ "$RC" = 0 ] && content_is "$R1" "OLD-BUNDLE-BYTES" && ok "local warm run leaves a drifted tracked file alone (it may be your edit)" || bad "local warm run touched the file or failed ($RC)"
case "$OUT" in *"differ from the index"*"tracked-Data.db.jsonl"*) ok "local warm run names the drifted tracked file" ;; *) bad "drift not reported locally: $OUT" ;; esac

# 4. explicit opt-in restores it locally
run_fetch "$R1" CQLITE_DATASETS_RESTORE_TRACKED=1
[ "$RC" = 0 ] && content_is "$R1" "$NEW" && ok "CQLITE_DATASETS_RESTORE_TRACKED=1 restores locally" || bad "opt-in restore failed ($RC)"

# 5. a cache from before #4508 (no manifest): still restored in CI; extras not classified
rm -f "$R1/test-data/datasets/.dataset-manifest"; overlay_bundle_over_checkout "$R1"
run_fetch "$R1" CI=true
[ "$RC" = 0 ] && content_is "$R1" "$NEW" && ok "pre-#4508 cache (no manifest): tracked bytes still restored in CI" || bad "pre-manifest cache not restored ($RC)"
case "$OUT" in *"cannot be classified"*) ok "pre-#4508 cache: extras are declared unclassifiable, not guessed" ;; *) bad "no unclassifiable note: $OUT" ;; esac

echo "test_fetch_datasets_warm_overlay: pass=$PASS fail=$FAIL"
[ "$FAIL" = 0 ]
