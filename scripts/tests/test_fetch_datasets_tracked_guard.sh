#!/usr/bin/env bash
#
# Regression tests for test-data/scripts/fetch-datasets.sh's tracked-fixture
# guard (issue #2878).
#
# The script does `rm -rf "${DATASET_ROOT}"` before extracting the pinned
# archive, and the archive is NOT a superset of the checkout: ~875 files under
# test-data/datasets are git-TRACKED (JSONL sstabledump goldens, force-added
# byte-parity *.db references, the #2389 commitlog fixtures) and some ship in no
# archive at all. Before #2878 the restore path never ran — the local arm was
# `CI`-gated off entirely, and the CI arm took a silent `return 0` from a raw
# string prefix compare — so a fetch DELETED tracked fixtures, red-ing the gate
# on a pristine main and leaving stageable deletions in the checkout.
#
# HERMETIC: no network, no real dataset, nothing outside a mktemp sandbox. Each
# case builds a throwaway `git init` repo with a fake tracked datasets/ subtree
# plus a tiny locally-built .tar.gz whose contents only PARTIALLY overlap it, and
# shadows `curl` with a stub that copies that tarball to the -o target. Every
# other step of the script is REAL: the sha256 verification, the `rm -rf`, the
# tar extraction, the restore, has_required_content and write_pin. The real
# test-data/datasets of this checkout is never read, written or deleted.
#
# Run standalone:   bash scripts/tests/test_fetch_datasets_tracked_guard.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
FETCH="$REPO_ROOT/test-data/scripts/fetch-datasets.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

[ -f "$FETCH" ] || { echo "FAIL - missing $FETCH"; exit 1; }

T=$(mktemp -d "${TMPDIR:-/tmp}/fetch-datasets-guard.XXXXXX")
ASSET="cqlite-2878-fake-$$.tar.gz"
# ASSET_PATH inside the script is hardcoded to /tmp/<asset>; the stub curl writes
# there, so clean it up with the sandbox.
trap 'rm -rf "$T" "/tmp/$ASSET"' EXIT

# ---------------------------------------------------------------------------
# The fake archive: same top-level layout as the real asset
# (test-data/datasets/...). Deliberately ships NO commitlog/ and NO *.jsonl
# goldens outside sstables/, and ships a STALE copy of one tracked reference
# binary (the committed copy must win).
# ---------------------------------------------------------------------------
WIDE_DIR="sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
ARCHIVE_SRC="$T/archive-src"
AD="$ARCHIVE_SRC/test-data/datasets"
mkdir -p "$AD/sstables/test_basic/simple_table-aaaa" "$AD/$WIDE_DIR" "$AD/corruption"
printf 'fake: archive metadata\n' >"$AD/metadata.yml"
for suffix in Data.db Index.db Summary.db Statistics.db; do
  printf 'archive %s\n' "$suffix" >"$AD/sstables/test_basic/simple_table-aaaa/nb-1-big-$suffix"
done
for suffix in Data.db Index.db Digest.crc32 CompressionInfo.db; do
  printf 'archive wide %s\n' "$suffix" >"$AD/$WIDE_DIR/nb-2-big-$suffix"
done
printf '{"archive":"wide golden"}\n' >"$AD/$WIDE_DIR/nb-2-big-Data.db.jsonl"
ARCHIVE_STALE_CONTENT='STALE-copy-from-archive'
printf '%s\n' "$ARCHIVE_STALE_CONTENT" >"$AD/corruption/committed-Data.db"

TARBALL="$T/$ASSET"
tar -czf "$TARBALL" -C "$ARCHIVE_SRC" test-data

SHA=""
if command -v sha256sum >/dev/null 2>&1; then
  SHA="$(sha256sum "$TARBALL" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  SHA="$(shasum -a 256 "$TARBALL" | awk '{print $1}')"
fi
if [ -n "$SHA" ]; then
  ok "fixture: computed real sha256 of the fake archive (checksum verification is exercised)"
else
  printf 'INFO - no sha256 tool; the script will warn-and-continue (CI unset)\n'
  SHA="0000000000000000000000000000000000000000000000000000000000000000"
fi

# --- stub curl ---------------------------------------------------------------
BIN="$T/bin"
mkdir -p "$BIN"
cat >"$BIN/curl" <<'MOCK'
#!/usr/bin/env bash
out=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="${2:-}"; shift 2 ;;
    *) shift ;;
  esac
done
if [ -z "$out" ]; then
  echo "stub curl: no -o target" >&2
  exit 1
fi
if [ -z "${STUB_CURL_PAYLOAD:-}" ] || [ ! -f "${STUB_CURL_PAYLOAD}" ]; then
  echo "stub curl: STUB_CURL_PAYLOAD unset or missing" >&2
  exit 1
fi
cp "${STUB_CURL_PAYLOAD}" "$out"
MOCK
chmod +x "$BIN/curl"

# ---------------------------------------------------------------------------
# make_repo <dir> — a throwaway checkout with a tracked datasets/ subtree that
# only partially overlaps the archive. Tracked files (relative to the repo):
#   test-data/datasets/commitlog/clean-CommitLog.log        archive ships NOTHING here (#2389 class)
#   test-data/datasets/commitlog/commitlog-ground-truth.json
#   test-data/datasets/goldens/simple_table-Data.db.jsonl   archive ships nothing here
#   test-data/datasets/goldens/spaced name-Data.db.jsonl    NUL-safety: a space in the name
#   test-data/datasets/corruption/committed-Data.db         archive ships a STALE copy
# The .gitignore mirrors the real repo (binary/archive content is ignored, the
# reference copies are force-added) so `git status --porcelain` emptiness is a
# meaningful post-condition.
# ---------------------------------------------------------------------------
TRACKED_RELATIVE=(
  "commitlog/clean-CommitLog.log"
  "commitlog/commitlog-ground-truth.json"
  "goldens/simple_table-Data.db.jsonl"
  "goldens/spaced name-Data.db.jsonl"
  "corruption/committed-Data.db"
)
committed_content() { printf 'COMMITTED-tracked-fixture:%s\n' "$1"; }

make_repo() {
  local dir="$1" rel
  mkdir -p "$dir"
  git -C "$dir" init -q
  git -C "$dir" config user.email test@example.com
  git -C "$dir" config user.name "Test"
  cat >"$dir/.gitignore" <<'IGN'
*.db
test-data/datasets/metadata.yml
test-data/datasets/sstables/
test-data/datasets/.dataset-pin
IGN
  for rel in "${TRACKED_RELATIVE[@]}"; do
    mkdir -p "$dir/test-data/datasets/$(dirname "$rel")"
    committed_content "$rel" >"$dir/test-data/datasets/$rel"
  done
  git -C "$dir" add .gitignore
  for rel in "${TRACKED_RELATIVE[@]}"; do
    git -C "$dir" add -f "test-data/datasets/$rel"
  done
  git -C "$dir" commit -qm "tracked dataset fixtures"
}

# run_fetch <cwd> <dataset-root> [extra env assignments...] — invoke the REAL
# script with curl stubbed and CI explicitly unset unless an override is passed.
# A dataset-root of "-" leaves CQLITE_DATASETS_ROOT UNSET so the script's
# documented relative default (test-data/datasets) is exercised.
# Sets $OUT (combined output) and $RC.
run_fetch() {
  local cwd="$1" root="$2"
  shift 2
  OUT=$(
    cd "$cwd" || exit 90
    unset CI GITHUB_ACTIONS CQLITE_DATASETS_ALLOW_UNPROTECTED
    export PATH="$BIN:$PATH"
    export STUB_CURL_PAYLOAD="$TARBALL"
    if [ "$root" = "-" ]; then
      env -u CQLITE_DATASETS_ROOT "$@" \
        DATASET_TAG="fake-tag" \
        DATASET_ASSET="$ASSET" \
        DATASET_SHA256="$SHA" \
        bash "$FETCH" 2>&1
    else
      env "$@" \
        CQLITE_DATASETS_ROOT="$root" \
        DATASET_TAG="fake-tag" \
        DATASET_ASSET="$ASSET" \
        DATASET_SHA256="$SHA" \
        bash "$FETCH" 2>&1
    fi
  )
  RC=$?
}

# assert_tracked_intact <repo> <label> — every tracked fixture is present with its
# COMMITTED content, and the checkout is clean.
assert_tracked_intact() {
  local repo="$1" label="$2" rel missing="" wrong="" dirty
  for rel in "${TRACKED_RELATIVE[@]}"; do
    if [ ! -f "$repo/test-data/datasets/$rel" ]; then
      missing="$missing $rel"
    elif [ "$(cat "$repo/test-data/datasets/$rel")" != "$(committed_content "$rel")" ]; then
      wrong="$wrong $rel"
    fi
  done
  if [ -n "$missing" ]; then
    bad "$label: tracked fixtures DELETED:$missing"
  else
    ok "$label: all ${#TRACKED_RELATIVE[@]} tracked fixtures survive the rm -rf"
  fi
  if [ -n "$wrong" ]; then
    bad "$label: committed content lost to the archive copy:$wrong"
  else
    ok "$label: committed content wins over the archive's stale copy"
  fi
  dirty="$(git -C "$repo" status --porcelain 2>&1)"
  if [ -z "$dirty" ]; then
    ok "$label: git status --porcelain is EMPTY (issue #2878 acceptance oracle)"
  else
    bad "$label: checkout dirty after fetch:"
    printf '     %s\n' "$dirty"
  fi
}

# assert_archive_extracted <root> <label> — the destructive extract really ran, so
# the preservation above is not the trivial "did nothing" pass.
assert_archive_extracted() {
  local root="$1" label="$2"
  if [ -f "$root/metadata.yml" ] && [ -s "$root/$WIDE_DIR/nb-2-big-Data.db.jsonl" ]; then
    ok "$label: archive content extracted (rm -rf + extract path really ran)"
  else
    bad "$label: archive content missing under $root"
  fi
}

# === Case 1: local arm (CI UNSET) — the destructive arm before #2878 ==========
R1="$T/case1-repo"
make_repo "$R1"
run_fetch "$R1" "$R1/test-data/datasets"
if [ "$RC" -eq 0 ]; then
  ok "CI-unset: fetch exits 0"
else
  bad "CI-unset: fetch exited $RC"
  printf '     %s\n' "$OUT"
fi
assert_tracked_intact "$R1" "CI-unset"
assert_archive_extracted "$R1/test-data/datasets" "CI-unset"
case "$OUT" in
  *"Restoring "*"git-tracked file"*) ok "CI-unset: restore actually reported work (not a silent no-op)" ;;
  *) bad "CI-unset: no restore reported; output: $OUT" ;;
esac

# === Case 1a: the DOCUMENTED DEFAULT spelling (CQLITE_DATASETS_ROOT unset) ====
# `bash test-data/scripts/fetch-datasets.sh` from a checkout root — the exact
# invocation in CLAUDE.md that destroyed tracked fixtures before #2878.
R1A="$T/case1a-repo"
make_repo "$R1A"
run_fetch "$R1A" "-"
if [ "$RC" -eq 0 ]; then
  ok "default-root: fetch exits 0 with CQLITE_DATASETS_ROOT unset"
else
  bad "default-root: fetch exited $RC"
  printf '     %s\n' "$OUT"
fi
assert_tracked_intact "$R1A" "default-root"
assert_archive_extracted "$R1A/test-data/datasets" "default-root"

# === Case 1b: NON-VACUITY — the same fixture with the guard neutered =========
# Mutant: the two `capture_tracked_dataset_files` call sites become no-ops, which
# reproduces the pre-#2878 behaviour (no capture -> nothing to restore). If the
# assertions above can also pass here, they prove nothing.
MUTANT="$T/fetch-datasets-mutant.sh"
sed 's/^capture_tracked_dataset_files$/: mutant-disabled/' "$FETCH" >"$MUTANT"
if grep -q ': mutant-disabled' "$MUTANT"; then
  ok "non-vacuity: built a guard-disabled mutant"
  R1B="$T/case1b-repo"
  make_repo "$R1B"
  FETCH_SAVED="$FETCH"
  FETCH="$MUTANT"
  run_fetch "$R1B" "$R1B/test-data/datasets"
  FETCH="$FETCH_SAVED"
  MUT_MISSING=0
  for rel in "${TRACKED_RELATIVE[@]}"; do
    [ -f "$R1B/test-data/datasets/$rel" ] || MUT_MISSING=$((MUT_MISSING + 1))
  done
  if [ "$MUT_MISSING" -gt 0 ]; then
    ok "non-vacuity: mutant DELETES $MUT_MISSING tracked fixture(s) (the #2878 defect)"
  else
    bad "non-vacuity: mutant preserved everything — the test cannot detect the defect"
  fi
  if [ -n "$(git -C "$R1B" status --porcelain 2>&1)" ]; then
    ok "non-vacuity: mutant leaves the checkout dirty with tracked-file deletions"
  else
    bad "non-vacuity: mutant left a clean checkout — assertion is vacuous"
  fi
else
  bad "non-vacuity: could not build the guard-disabled mutant (call site renamed?)"
fi

# === Case 2: CI SET — the other arm, same code path ==========================
R2="$T/case2-repo"
make_repo "$R2"
run_fetch "$R2" "$R2/test-data/datasets" CI=true
if [ "$RC" -eq 0 ]; then
  ok "CI=true: fetch exits 0"
else
  bad "CI=true: fetch exited $RC"
  printf '     %s\n' "$OUT"
fi
assert_tracked_intact "$R2" "CI=true"

# === Case 3: in-repo via a SYMLINK + `..` segments ===========================
# A raw string prefix compare against the unresolved spelling is what silently
# skipped the pre-#2878 restore.
R3="$T/case3-repo"
make_repo "$R3"
ln -s "$R3" "$T/case3-link"
run_fetch "$R3" "$T/case3-link/test-data/../test-data/datasets"
if [ "$RC" -eq 0 ]; then
  ok "symlink+..: fetch exits 0"
else
  bad "symlink+..: fetch exited $RC"
  printf '     %s\n' "$OUT"
fi
assert_tracked_intact "$R3" "symlink+.."
assert_archive_extracted "$R3/test-data/datasets" "symlink+.."

# === Case 4: OUT-OF-REPO dataset dir — must work, must not git-restore =======
R4="$T/case4-repo"
make_repo "$R4"
OUTSIDE="$T/case4-outside"
mkdir -p "$OUTSIDE/test-data"
if git -C "$OUTSIDE" rev-parse --show-toplevel >/dev/null 2>&1; then
  printf 'INFO - TMPDIR is inside a git checkout; skipping the out-of-repo case\n'
else
  run_fetch "$R4" "$OUTSIDE/test-data/datasets"
  if [ "$RC" -eq 0 ]; then
    ok "out-of-repo: fetch exits 0"
  else
    bad "out-of-repo: fetch exited $RC"
    printf '     %s\n' "$OUT"
  fi
  assert_archive_extracted "$OUTSIDE/test-data/datasets" "out-of-repo"
  case "$OUT" in
    *"Restoring "*"git-tracked file"*) bad "out-of-repo: tried to git-restore an out-of-repo dataset dir" ;;
    *) ok "out-of-repo: no git restore attempted" ;;
  esac
  if [ -z "$(git -C "$R4" status --porcelain 2>&1)" ]; then
    ok "out-of-repo: the cwd repo is untouched"
  else
    bad "out-of-repo: the cwd repo was modified"
  fi
fi

# === Case 5: component-wise containment, not string prefix ===================
# "<repo>-sibling" shares a string prefix with "<repo>" but is NOT inside it. A
# prefix compare would classify it in-repo and derive a bogus relative path.
R5="$T/case5-repo"
make_repo "$R5"
SIBLING="$T/case5-repo-sibling"
mkdir -p "$SIBLING/test-data"
run_fetch "$R5" "$SIBLING/test-data/datasets"
if [ "$RC" -eq 0 ]; then
  ok "sibling-prefix: fetch exits 0 (not misclassified as in-repo)"
else
  bad "sibling-prefix: fetch exited $RC"
  printf '     %s\n' "$OUT"
fi
assert_archive_extracted "$SIBLING/test-data/datasets" "sibling-prefix"
if [ -z "$(git -C "$R5" status --porcelain 2>&1)" ]; then
  ok "sibling-prefix: the neighbouring repo is untouched"
else
  bad "sibling-prefix: the neighbouring repo was modified"
fi

# === Case 6: cannot determine -> REFUSE loudly, never a silent bail ==========
# A directory that carries a .git entry but is not a usable work tree: the guard
# cannot enumerate tracked files, so it must refuse BEFORE the rm -rf.
R6="$T/case6-broken"
mkdir -p "$R6/.git" "$R6/test-data/datasets/commitlog"
CANARY="$R6/test-data/datasets/commitlog/canary.log"
printf 'canary must survive a refusal\n' >"$CANARY"
run_fetch "$R6" "$R6/test-data/datasets"
if [ "$RC" -ne 0 ]; then
  ok "undeterminable: fetch fails closed (exit $RC)"
else
  bad "undeterminable: fetch exited 0 — a silent bail on a destructive path"
fi
case "$OUT" in
  *"#2878"*) ok "undeterminable: error names issue #2878" ;;
  *) bad "undeterminable: error does not cite #2878; output: $OUT" ;;
esac
case "$OUT" in
  *"$R6/test-data/datasets"*) ok "undeterminable: error names the dataset path it refused" ;;
  *) bad "undeterminable: error does not name the path; output: $OUT" ;;
esac
if [ -f "$CANARY" ]; then
  ok "undeterminable: refused BEFORE deleting anything"
else
  bad "undeterminable: the dataset dir was destroyed despite the refusal"
fi

# === Case 7: the refusal is opt-out-able, loudly ============================
run_fetch "$R6" "$R6/test-data/datasets" CQLITE_DATASETS_ALLOW_UNPROTECTED=1
if [ "$RC" -eq 0 ]; then
  ok "opt-out: CQLITE_DATASETS_ALLOW_UNPROTECTED=1 proceeds"
else
  bad "opt-out: still failed (exit $RC); output: $OUT"
fi
case "$OUT" in
  *"WARNING"*"may be DELETED"*) ok "opt-out: warns that tracked fixtures may be deleted" ;;
  *) bad "opt-out: no loud warning; output: $OUT" ;;
esac

# --- summary -----------------------------------------------------------------
printf '\n%s: %d passed, %d failed\n' "$(basename "$0")" "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
