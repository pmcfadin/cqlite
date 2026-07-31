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

# A BAD archive for the abort-window case: valid gzip/tar, but its top level is
# not test-data/datasets, so the script hits its explicit `exit 1` AFTER the
# `rm -rf` — the exact window that used to lose the fixtures for good.
BAD_SRC="$T/bad-archive-src"
mkdir -p "$BAD_SRC/wrong-root/datasets"
printf 'not the expected layout\n' >"$BAD_SRC/wrong-root/datasets/thing.txt"
BAD_TARBALL="$T/bad-$ASSET"
tar -czf "$BAD_TARBALL" -C "$BAD_SRC" wrong-root

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}
SHA="$(sha256_of "$TARBALL")"
BAD_SHA="$(sha256_of "$BAD_TARBALL")"
if [ -n "$SHA" ]; then
  ok "fixture: computed real sha256 of the fake archive (checksum verification is exercised)"
else
  printf 'INFO - no sha256 tool; the script will warn-and-continue (CI unset)\n'
  SHA="0000000000000000000000000000000000000000000000000000000000000000"
  BAD_SHA="$SHA"
fi

# Cases that require a path to be OUTSIDE any git work tree are only meaningful
# when the sandbox itself is not inside a checkout — git discovery would otherwise
# find the enclosing repo. Probe ONCE and reuse (a test whose verdict depends on
# where TMPDIR points is a flake, and gate flakes cost everyone).
SANDBOX_IN_REPO=0
if git -C "$T" rev-parse --show-toplevel >/dev/null 2>&1; then
  SANDBOX_IN_REPO=1
  printf 'INFO - TMPDIR (%s) is inside a git checkout; out-of-repo cases are skipped\n' "$T"
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
# documented relative default (test-data/datasets) is exercised. Set
# FETCH_PAYLOAD/FETCH_PAYLOAD_SHA to serve a different archive.
# Sets $OUT (combined output) and $RC.
FETCH_PAYLOAD=""
FETCH_PAYLOAD_SHA=""
FETCH_BIN=""
run_fetch() {
  local cwd="$1" root="$2"
  shift 2
  local payload="${FETCH_PAYLOAD:-$TARBALL}" sha="${FETCH_PAYLOAD_SHA:-$SHA}"
  local bin="${FETCH_BIN:-$BIN}"
  OUT=$(
    cd "$cwd" || exit 90
    unset CI GITHUB_ACTIONS CQLITE_DATASETS_ALLOW_UNPROTECTED
    export PATH="$bin:$PATH"
    export STUB_CURL_PAYLOAD="$payload"
    if [ "$root" = "-" ]; then
      env -u CQLITE_DATASETS_ROOT "$@" \
        DATASET_TAG="fake-tag" \
        DATASET_ASSET="$ASSET" \
        DATASET_SHA256="$sha" \
        bash "$FETCH" 2>&1
    else
      env "$@" \
        CQLITE_DATASETS_ROOT="$root" \
        DATASET_TAG="fake-tag" \
        DATASET_ASSET="$ASSET" \
        DATASET_SHA256="$sha" \
        bash "$FETCH" 2>&1
    fi
  )
  RC=$?
}

# assert_refusal <label> <expected-substring> <canary-path> — the run must fail
# closed, say why, and have deleted nothing.
assert_refusal() {
  local label="$1" needle="$2" canary="$3"
  if [ "$RC" -ne 0 ]; then
    ok "$label: fails closed (exit $RC)"
  else
    bad "$label: exited 0 — a destructive path was taken silently"
  fi
  case "$OUT" in
    *"$needle"*) ok "$label: error explains the refusal ('$needle')" ;;
    *) bad "$label: error missing '$needle'; output: $OUT" ;;
  esac
  if [ -e "$canary" ]; then
    ok "$label: refused BEFORE deleting anything"
  else
    bad "$label: '$canary' was destroyed despite the refusal"
  fi
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
if [ "$SANDBOX_IN_REPO" = 1 ]; then
  printf 'INFO - skipping the out-of-repo case (sandbox is inside a checkout)\n'
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
if [ "$SANDBOX_IN_REPO" = 1 ]; then
  printf 'INFO - skipping the sibling-prefix case (sandbox is inside a checkout)\n'
else
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
fi

# === Case 6: cannot determine -> REFUSE loudly, never a silent bail ==========
# A directory that carries a .git entry but is not a usable work tree: the guard
# cannot enumerate tracked files, so it must refuse BEFORE the rm -rf. Needs the
# sandbox to be outside a checkout — git 2.43 discovery walks PAST an invalid
# .git to an enclosing repo, which would make the dir classifiable after all.
R6="$T/case6-broken"
mkdir -p "$R6/.git" "$R6/test-data/datasets/commitlog"
CANARY="$R6/test-data/datasets/commitlog/canary.log"
printf 'canary must survive a refusal\n' >"$CANARY"
if [ "$SANDBOX_IN_REPO" = 1 ]; then
  printf 'INFO - skipping the undeterminable + opt-out cases (sandbox is inside a checkout)\n'
else
  run_fetch "$R6" "$R6/test-data/datasets"
  assert_refusal "undeterminable" "#2878" "$CANARY"
  case "$OUT" in
    *"$R6/test-data/datasets"*) ok "undeterminable: error names the dataset path it refused" ;;
    *) bad "undeterminable: error does not name the path; output: $OUT" ;;
  esac

  # === Case 7: the refusal is opt-out-able, loudly ==========================
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
fi

# === Case 8: ABORT AFTER the rm -rf must still restore (BLOCKER 1) ===========
# A valid tarball with an unexpected top level hits the script's explicit
# `exit 1` AFTER the dataset dir has been deleted. Every other abort in that
# window (tar failure, ENOSPC on the mv, SIGINT mid-extract) leaves the same
# state, and before the fix all of them lost the fixtures permanently while the
# error message blamed the archive.
R8="$T/case8-repo"
make_repo "$R8"
FETCH_PAYLOAD="$BAD_TARBALL"
FETCH_PAYLOAD_SHA="$BAD_SHA"
run_fetch "$R8" "$R8/test-data/datasets"
FETCH_PAYLOAD=""
FETCH_PAYLOAD_SHA=""
if [ "$RC" -ne 0 ]; then
  ok "abort-window: the bad archive still fails the run (exit $RC)"
else
  bad "abort-window: bad archive accepted; output: $OUT"
fi
case "$OUT" in
  *"did not contain test-data/datasets"*) ok "abort-window: the original archive error is preserved" ;;
  *) bad "abort-window: archive error lost; output: $OUT" ;;
esac
case "$OUT" in
  *"aborted"*"restoring the git-tracked reference fixtures"*)
    ok "abort-window: the abort path announces the restore it performed" ;;
  *) bad "abort-window: no abort-restore announcement; output: $OUT" ;;
esac
assert_tracked_intact "$R8" "abort-window"
if [ ! -f "$R8/test-data/datasets/metadata.yml" ]; then
  ok "abort-window: no archive content claimed (the failure is not papered over)"
else
  bad "abort-window: archive content present after a failed extraction"
fi

# --- Case 8b: NON-VACUITY for the abort window -------------------------------
# Flipping the flag that arms the abort-path restore reproduces the pre-fix loss.
MUTANT_NO_ABORT="$T/fetch-datasets-mutant-no-abort.sh"
sed 's/^TRACKED_GUARD_DESTRUCTIVE_STARTED=1$/TRACKED_GUARD_DESTRUCTIVE_STARTED=0/' "$FETCH" >"$MUTANT_NO_ABORT"
if ! cmp -s "$FETCH" "$MUTANT_NO_ABORT"; then
  ok "non-vacuity: built an abort-restore-disabled mutant"
  R8B="$T/case8b-repo"
  make_repo "$R8B"
  FETCH_SAVED="$FETCH"
  FETCH="$MUTANT_NO_ABORT"
  FETCH_PAYLOAD="$BAD_TARBALL"
  FETCH_PAYLOAD_SHA="$BAD_SHA"
  run_fetch "$R8B" "$R8B/test-data/datasets"
  FETCH="$FETCH_SAVED"
  FETCH_PAYLOAD=""
  FETCH_PAYLOAD_SHA=""
  MUT8_MISSING=0
  for rel in "${TRACKED_RELATIVE[@]}"; do
    [ -f "$R8B/test-data/datasets/$rel" ] || MUT8_MISSING=$((MUT8_MISSING + 1))
  done
  if [ "$MUT8_MISSING" -eq "${#TRACKED_RELATIVE[@]}" ]; then
    ok "non-vacuity: mutant loses ALL $MUT8_MISSING tracked fixtures in the abort window"
  else
    bad "non-vacuity: mutant lost only $MUT8_MISSING fixtures — the abort assert is weak"
  fi
  if [ -n "$(git -C "$R8B" status --porcelain 2>&1)" ]; then
    ok "non-vacuity: mutant leaves the checkout dirty after the aborted fetch"
  else
    bad "non-vacuity: mutant left a clean checkout — the abort assert is vacuous"
  fi
else
  bad "non-vacuity: could not build the abort-restore-disabled mutant (flag renamed?)"
fi

# === Case 8c: the SIGNAL arm of the abort window =============================
# The window is also reachable by Ctrl-C / SIGTERM mid-extract, which a bare EXIT
# trap alone would not cover. Driven DETERMINISTICALLY (no sleeps, no wall-clock):
# a stub `tar` signals the fetch script itself, so bash runs the trap at the very
# next command boundary. Nothing here depends on timing.
SIGBIN="$T/bin-signal"
mkdir -p "$SIGBIN"
cp "$BIN/curl" "$SIGBIN/curl"
cat >"$SIGBIN/tar" <<'MOCK'
#!/usr/bin/env bash
# Stand in for an interrupted extraction: signal the invoking fetch script and
# exit as a killed-by-signal tar would.
kill -TERM "$PPID" 2>/dev/null
exit 143
MOCK
chmod +x "$SIGBIN/tar"
R8C="$T/case8c-repo"
make_repo "$R8C"
FETCH_BIN="$SIGBIN"
run_fetch "$R8C" "$R8C/test-data/datasets"
FETCH_BIN=""
if [ "$RC" -ne 0 ]; then
  ok "abort-signal: an interrupted extraction still fails the run (exit $RC)"
else
  bad "abort-signal: interrupted extraction reported success; output: $OUT"
fi
case "$OUT" in
  *"aborted"*"restoring the git-tracked reference fixtures"*)
    ok "abort-signal: the signal path runs the same abort restore" ;;
  *) bad "abort-signal: no abort-restore announcement; output: $OUT" ;;
esac
assert_tracked_intact "$R8C" "abort-signal"

# === Case 9: the target IS a repository root (BLOCKER 2) =====================
# `rm -rf` would take .git with it, so the index the guard restores from would be
# gone too. cwd is deliberately outside any repo so this exercises the guard, not
# canonicalize_dataset_root's cwd-repo check.
if [ "$SANDBOX_IN_REPO" = 1 ]; then
  printf 'INFO - skipping the repo-root / nested-checkout / HOME cases (sandbox is inside a checkout)\n'
else
  R9="$T/case9/datasets"
  mkdir -p "$R9"
  git -C "$R9" init -q
  git -C "$R9" config user.email test@example.com
  git -C "$R9" config user.name "Test"
  printf 'in a repo whose root is named datasets\n' >"$R9/keep.txt"
  git -C "$R9" add keep.txt
  git -C "$R9" commit -qm "keep"
  run_fetch "$T" "$R9"
  assert_refusal "repo-root-target" "is itself a git repository root" "$R9/.git"
  if [ -f "$R9/keep.txt" ]; then
    ok "repo-root-target: the repository's own content survives"
  else
    bad "repo-root-target: repository content was deleted"
  fi

  # === Case 10: a nested checkout BENEATH an out-of-repo target =============
  # An out-of-repo dataset dir is not a free pass: someone else's checkout inside
  # it is just as unrecoverable.
  NEST="$T/case10/test-data/datasets"
  mkdir -p "$NEST/vendor/other-checkout"
  printf 'keep\n' >"$NEST/keep.txt"
  git -C "$NEST/vendor/other-checkout" init -q
  git -C "$NEST/vendor/other-checkout" config user.email test@example.com
  git -C "$NEST/vendor/other-checkout" config user.name "Test"
  printf 'someone else work\n' >"$NEST/vendor/other-checkout/work.txt"
  git -C "$NEST/vendor/other-checkout" add work.txt
  git -C "$NEST/vendor/other-checkout" commit -qm "work"
  run_fetch "$T" "$NEST"
  assert_refusal "nested-checkout" "contains a nested git repository" "$NEST/vendor/other-checkout/.git"
  if [ -f "$NEST/vendor/other-checkout/work.txt" ] && [ -f "$NEST/keep.txt" ]; then
    ok "nested-checkout: the nested repository's content survives"
  else
    bad "nested-checkout: the nested checkout was destroyed"
  fi

  # === Case 11: the target is an ANCESTOR of a checkout =====================
  ANC="$T/case11/datasets"
  mkdir -p "$ANC"
  make_repo "$ANC/inner-repo"
  run_fetch "$T" "$ANC"
  assert_refusal "ancestor-of-repo" "#2878" "$ANC/inner-repo/.git"

  # === Case 12: DATASET_ROOT == $HOME ======================================
  HOMEISH="$T/case12/datasets"
  mkdir -p "$HOMEISH"
  printf 'home content\n' >"$HOMEISH/keep.txt"
  run_fetch "$T" "$HOMEISH" HOME="$HOMEISH"
  assert_refusal "home-target" "refusing to replace HOME" "$HOMEISH/keep.txt"
fi

# === Case 13: DATASET_ROOT == / =============================================
run_fetch "$T" "/"
assert_refusal "root-target" "unsafe CQLITE_DATASETS_ROOT" "/etc"

# --- summary -----------------------------------------------------------------
printf '\n%s: %d passed, %d failed\n' "$(basename "$0")" "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
