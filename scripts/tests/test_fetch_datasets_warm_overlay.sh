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
  printf '# sha256=%s\ngoldens/old-name-Data.db.jsonl\ngoldens/removed-Data.db.jsonl\ngoldens/tracked-Data.db.jsonl\n' "$SHA" > "$d/test-data/dataset-bundle-tracked.txt"
  git -C "$d" add .gitignore test-data/dataset-bundle-tracked.txt; git -C "$d" add -f "$DS/goldens"
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
[ ! -e "$B" ] && ok "the twin list is no longer cache-derived: no .dataset-tracked-at-build is written" || bad "a cache-derived .dataset-tracked-at-build was written"

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
rm -f "$R/$DS/.dataset-manifest"
run_fetch "$R"
{ [ "$RC" = 0 ] && case "$OUT" in *"cannot be classified"*) true ;; *) false ;; esac; } && ok "pre-#4508 cache, local: noted, not guessed" || bad "pre-manifest local run: $OUT"
git -C "$R" checkout -q -- "$DS" 2>/dev/null
run_fetch "$R" CI=true
case "$OUT" in *"forcing a COLD restore"*"Fetching dataset"*) ok "pre-#4508 cache, CI: refuses the warm path and runs the cold restore" ;; *) bad "no cold restore: $OUT" ;; esac
{ [ "$RC" = 0 ] && tracked_is "$R" "$NEW" && [ -s "$R/$DS/.dataset-manifest" ]; } \
  && ok "after the forced cold restore the tracked bytes are the checkout's and the manifest exists" || bad "cold restore left a bad state ($RC)"

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
printf 'x\n' > "$T/r/$DS/.dataset-manifest"; : 
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

# 9. NO workflow restores the dataset cache without an unconditional reconcile after it (roborev job 197): on an exact
#    cache hit a `cache-hit != 'true'`-guarded fetch is skipped, so fetch-datasets.sh (which restores the tracked
#    files and removes stale twins) would never run. Every cache site must be followed, in its job, by the composite
#    action or a fetch-datasets.sh step whose `if:` mentions neither cache-hit nor runner.os: an OS-gated fetch leaves
#    that OS (Windows, under Git Bash) unreconciled, so it does not count (roborev job 198).
read -r -d '' STEP_PY <<'PY' || true
import glob, re, sys
root = sys.argv[1]
bad, sites = [], 0
for f in sorted(glob.glob(root + "/.github/workflows/*.yml")):
    text = open(f).read()
    parts = re.split(r"(?m)^(?=      - (?:name|uses):)", text)
    for i, st in enumerate(parts):
        if "actions/cache" in st and re.search(r"datasets-fmt\d+-", st):
            sites += 1
            rest = []
            for nxt in parts[i + 1:]:
                if "actions/cache" in nxt and re.search(r"datasets-fmt\d+-", nxt):
                    break
                rest.append(nxt)
            ok_fetch, os_gated, reassert = False, False, False
            for st2 in rest:
                if "restore-canonical-datasets" in st2:
                    ok_fetch = True
                m = re.search(r"(?m)^\s+if:(.*)$", st2)
                cond = m.group(1) if m else ""
                code = "\n".join(l for l in st2.splitlines() if not l.lstrip().startswith("#"))
                if re.search(r"bash\s+\S*fetch-datasets\.sh", code) and "cache-hit" not in cond and "runner.os" not in cond:
                    ok_fetch = True
                if "git restore --source=HEAD -- test-data/datasets" in code and "cache-hit" not in cond:
                    reassert = True
            name = f.split("/")[-1]
            if not ok_fetch:
                bad.append(name + ": no unconditional fetch-datasets.sh / restore-canonical-datasets after the cache restore")
print(sites)
for b in bad:
    print("BAD " + b)
PY
step_out="$(python3 -c "$STEP_PY" "$REPO_ROOT")"
step_sites="$(head -1 <<<"$step_out")"
step_bad="$(grep '^BAD ' <<<"$step_out" || true)"
[ -z "$step_bad" ] && [ "${step_sites:-0}" -ge 15 ] && ok "every dataset cache restore ($step_sites sites) is followed by an unconditional reconcile" \
  || bad "a dataset cache restore lacks an unconditional reconcile (sites=$step_sites): $step_bad"

# 10. Git for Windows prints the paths it reports (toplevel, git dirs) in WINDOWS form (D:/a/x) and Git Bash maps them
#     back with cygpath (roborev job 199 / the real Windows CI failure). Simulate both: a `git` shim that prefixes a
#     drive letter on those rev-parse outputs, and a `cygpath -u` stub that strips it (and maps a fake Windows root).
REAL_GIT="$(command -v git)"
cat > "$BIN/git" <<SHIM
#!/usr/bin/env bash
# only the path-printing rev-parse calls are rewritten; every other call (ls-files -z, restore, diff) is the real git
case " \$* " in
  *" rev-parse "*)
    case " \$* " in
      *--show-toplevel*|*--absolute-git-dir*|*--git-common-dir*)
        out=\$("$REAL_GIT" "\$@"); rc=\$?
        [ -z "\$out" ] || out="D:\$out"
        printf '%s\n' "\$out"; exit \$rc ;;
    esac ;;
esac
exec "$REAL_GIT" "\$@"
SHIM
cat > "$BIN/cygpath" <<'CYG'
#!/bin/sh
[ "$1" = "-u" ] || exit 2
case "$2" in
  'C:\fake'*) printf '%s\n' "$STUB_CYGPATH_OUT" ;;
  [A-Za-z]:*) p="${2#?:}"; printf '%s\n' "$p" ;;
  *) printf '%s\n' "$2" ;;
esac
CYG
chmod +x "$BIN/git" "$BIN/cygpath"
R3="$T/r3"; make_repo "$R3"; run_fetch "$R3"
[ "$RC" = 0 ] && ok "cold run succeeds with git printing D:/... toplevel and git dirs" || bad "cold run with Windows-form git paths failed ($RC): $(tail -3 <<<"$OUT" | tr "\n" " ")"
overlay_cache "$R3"
run_fetch "$R3" CI=true
{ [ "$RC" = 0 ] && tracked_is "$R3" "$NEW"; } && ok "warm CI run reconciles with git printing Windows-form paths" || bad "warm run with Windows-form git paths failed ($RC): $(tail -3 <<<"$OUT" | tr "\n" " ")"
overlay_cache "$R3"
run_fetch "$R3" CI=true 'CQLITE_DATASETS_ROOT=C:\fake\checkout\test-data\datasets' STUB_CYGPATH_OUT="$R3/$DS"
{ [ "$RC" = 0 ] && tracked_is "$R3" "$NEW"; } && ok "a Windows-form CQLITE_DATASETS_ROOT is converted with cygpath and reconciled" || bad "windows-form root failed ($RC): $(tail -3 <<<"$OUT" | tr "\n" " ")"
rm -f "$BIN/git" "$BIN/cygpath"

# 11. ARG_MAX class (the real Windows CI failure): 925 tracked paths overflowed Windows' ~32 KB command line when handed to
#     `git restore` as argv. A git shim rejects any invocation whose arguments total more than 8 KiB; 700 long tracked paths
#     (~60 KB of pathspecs) must still restore, in both the missing-only pre-flight and the post-extraction `all` mode.
R5="$T/r5"; make_repo "$R5"
mkdir -p "$R5/$G/big"
for n in $(seq 1 700); do printf 'big %s\n' "$n" > "$R5/$G/big/file-$(printf '%04d' "$n")-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Data.db.jsonl"; done
git -C "$R5" add -f "$DS/goldens/big"; git -C "$R5" commit -qm "many tracked files"
cat > "$BIN/git" <<SHIM
#!/usr/bin/env bash
n=0; for a in "\$@"; do n=\$((n + \${#a} + 1)); done
if [ "\$n" -gt 8192 ]; then echo "git: Argument list too long" >&2; exit 126; fi
exec "$REAL_GIT" "\$@"
SHIM
chmod +x "$BIN/git"
run_fetch "$R5"
[ "$RC" = 0 ] && ok "cold run restores 700 long tracked paths under an 8 KiB argv limit (pathspecs on stdin)" || bad "cold run under the argv limit failed ($RC): $(tail -3 <<<"$OUT" | tr "\n" " ")"
find "$R5/$G/big" -name 'file-00[0-9][0-9]-*' -delete
run_fetch "$R5" CI=true
{ [ "$RC" = 0 ] && [ "$(find "$R5/$G/big" -type f | wc -l | tr -d ' ')" = 700 ]; } \
  && ok "the missing-only pre-flight and the CI reconcile repair 99 deleted tracked files under the argv limit" || bad "argv-limit warm run failed ($RC): $(tail -3 <<<"$OUT" | tr "\n" " ")"
rm -f "$BIN/git"

# 12. locale-proof patterns: a `[a-z]` range matches UPPERCASE letters under bash 3.2 (macOS) with a UTF-8 locale, so the
#     index-flag check read every ordinary `H` entry as assume-unchanged and refused all 925 tracked paths (the macOS CI
#     failure). Case patterns must use [[:lower:]] / [[:upper:]]; and no git call may take an array of paths as argv.
range_hits="$(grep -nE '^[[:space:]]*[^#[:space:]][^#]*(\[a-z\]|\[A-Z\])[^#]*\)[[:space:]]*$' "$REPO_ROOT/test-data/scripts/fetch-datasets.sh" "$REPO_ROOT/test-data/scripts/lib/dataset-warm-restore.sh" || true)"
[ -z "$range_hits" ] && ok "no locale-sensitive [a-z]/[A-Z] case range in fetch-datasets.sh or the warm-restore lib" || bad "locale-sensitive case range: $range_hits"
argv_hits="$(grep -nE '(^|[^#])[[:space:]](guard_git|git)[[:space:]].*"\$\{[A-Za-z_]+\[@\]' "$REPO_ROOT/test-data/scripts/fetch-datasets.sh" "$REPO_ROOT/test-data/scripts/lib/dataset-warm-restore.sh" | grep -v '^[^:]*:[0-9]*:[[:space:]]*#' | grep -v -e 'pri''ntf' -e TRACKED_PROBE_REASON -e report_ || true)"
[ -z "$argv_hits" ] && ok "no git call takes a path array as argv" || bad "git called with an array argv: $argv_hits"

# 13. roborev job 201 (1): a cache built COLD on the PR branch used to record the PR's tracked set, so a path the PR renamed
#     away looked like a legitimate bundle member forever. The twin list is the pinned bundle's own tracked set (committed,
#     sha-pinned), so a COLD extraction on the PR branch still removes the renamed-away and deleted twins.
R4="$T/r4"; make_repo "$R4"; pr_changes "$R4"
run_fetch "$R4" CI=true
{ [ "$RC" = 0 ] && ! has "$R4" removed-Data.db.jsonl && ! has "$R4" old-name-Data.db.jsonl && has "$R4" new-name-Data.db.jsonl && has "$R4" bundle-only.db && tracked_is "$R4" "$NEW"; } \
  && ok "COLD run on the PR branch: renamed-away and deleted twins are removed, the new name and the binary kept" || bad "cold PR run left twins or failed ($RC): $OUT"
case "$OUT" in *"Fetching dataset"*"REMOVED stale twin"*) ok "the twin removal ran on the cold path" ;; *) bad "no twin removal on the cold path: $OUT" ;; esac

# 14. roborev job 201 (2): a tracked path with a NEWLINE would split one NUL record into two lines, the second `../victim`.
NL=$'\n'
R6="$T/r6"; make_repo "$R6"; run_fetch "$R6"
mkdir -p "$R6/$G/X${NL}.."; printf 'hostile\n' > "$R6/$G/X${NL}../victim"
git -C "$R6" add -f "$DS/goldens"; git -C "$R6" commit -qm "hostile tracked name"
printf 'precious\n' > "$R6/test-data/victim"
run_fetch "$R6" CI=true
{ [ "$RC" != 0 ] && [ -f "$R6/test-data/victim" ]; } && ok "a tracked path with a newline is refused and nothing outside the dataset root is deleted" || bad "hostile newline path: rc=$RC victim=$([ -f "$R6/test-data/victim" ] && echo kept || echo DELETED)"
case "$OUT" in *"newline"*) ok "the refusal names the newline" ;; *) bad "refusal does not name the newline: $OUT" ;; esac
# the removal step itself validates every candidate, whatever produced it (a hostile manifest, a symlink)
mkdir -p "$T/rm/ds"; printf 'x\n' > "$T/rm/victim"; ln -s "$T/rm/victim" "$T/rm/ds/link"; printf 'y\n' > "$T/rm/ds/ok.txt"
rmcase() { OUT=$( DATASET_ROOT="$T/rm/ds" bash -c 'set -e; . "$0"; _dataset_remove_list x "$1"' "$REPO_ROOT/test-data/scripts/lib/dataset-warm-restore.sh" "$1" 2>&1 ); RC=$?; }
rmcase '../victim';       { [ "$RC" = 1 ] && [ -f "$T/rm/victim" ]; } && ok "a '..' path is refused, the file outside survives" || bad "dotdot path not refused (rc=$RC)"
rmcase "/$T/rm/victim";   { [ "$RC" = 1 ] && [ -f "$T/rm/victim" ]; } && ok "an absolute path is refused" || bad "absolute path not refused (rc=$RC)"
rmcase 'link';            { [ "$RC" = 1 ] && [ -f "$T/rm/victim" ]; } && ok "a symlink is refused (never followed)" || bad "symlink not refused (rc=$RC)"
rmcase 'ok.txt';          { [ "$RC" = 0 ] && [ ! -e "$T/rm/ds/ok.txt" ]; } && ok "an ordinary relative path is removed" || bad "ordinary path not removed (rc=$RC)"

# 15. roborev job 201 (3): a restore-key hit from an OLDER pin used to skip the full tracked restore (it only ran behind
#     has_required_dataset), so the cold path then rejected the overlaid tracked files as local modifications.
R7="$T/r7"; make_repo "$R7"; run_fetch "$R7"
overlay_cache "$R7"; sed 's/^sha256=.*/sha256=0000000000000000000000000000000000000000000000000000000000000000/' "$R7/$DS/.dataset-pin" > "$T/pin.n"; cp "$T/pin.n" "$R7/$DS/.dataset-pin"
tracked_is "$R7" "OLD-BUNDLE-BYTES" && ok "setup: older-pin cache overlaid the tracked file" || bad "setup failed"
run_fetch "$R7" CI=true
{ [ "$RC" = 0 ] && tracked_is "$R7" "$NEW"; } && ok "restore-key hit with an older pin: tracked files restored first, then the pin mismatch goes cold and succeeds" || bad "older-pin run failed ($RC): $(tail -3 <<<"$OUT" | tr "\n" " ")"
case "$OUT" in *"Fetching dataset"*) ok "the older pin took the cold path" ;; *) bad "older pin did not re-fetch: $OUT" ;; esac

# 16. roborev job 201 (4): the classifier runs inside `||` lists (errexit off); a failing sort must FAIL it, not yield partial lists.
R8="$T/r8"; make_repo "$R8"; pr_changes "$R8"; run_fetch "$R8" CI=true
overlay_cache "$R8"
REAL_SORT="$(command -v sort)"
printf '#!/bin/sh\ncase " $* " in *" -u "*) echo "sort: stub failure" >&2; exit 1 ;; esac\nexec "%s" "$@"\n' "$REAL_SORT" > "$BIN/sort"; chmod +x "$BIN/sort"
mkdir -p "$R8/$G"; printf 'old name\n' > "$R8/$G/old-name-Data.db.jsonl"
run_fetch "$R8" CI=true
rm -f "$BIN/sort"
{ [ "$RC" = 1 ] && case "$OUT" in *"could not classify"*) true ;; *) false ;; esac && has "$R8" old-name-Data.db.jsonl; } \
  && ok "a failing sort makes the classifier FAIL (rc 1, named); nothing is removed from partial lists" || bad "failing sort not fatal: rc=$RC $OUT"

# 17. roborev job 208 (1): the fetch must be a STANDALONE command in every workflow/action step that runs it. A shell-level
#     presence check (`[ -f x ] || bash fetch-datasets.sh`, `if ...; then bash ...; fi`, a case/loop) would let a complete
#     warm cache skip the reconcile. Read each step's run: body (comments stripped, `\` continuations joined), not its `if:`.
GUARD_PY='
import glob, re, sys, yaml
root = sys.argv[1]
ALLOWED = re.compile(r"^(bash scripts/ci/ci-timing-summary\.sh measure \"[^\"]*\" -- )?bash \.?/?(test-data/scripts/fetch-datasets\.sh)$")
seen, bad = 0, []
def scan(label, text):
    global seen
    logical, cur = [], ""
    for raw in text.splitlines():
        line = re.sub(r"(^|\s)#.*$", "", raw).rstrip()
        if line.endswith("\\"):
            cur += line[:-1] + " "; continue
        logical.append((cur + line).strip()); cur = ""
    if cur: logical.append(cur.strip())
    for l in logical:
        if "fetch-datasets.sh" not in l: continue
        seen += 1
        if not ALLOWED.match(" ".join(l.split())):
            bad.append(label + ": " + l)
def walk(label, node):
    if isinstance(node, dict):
        for k, v in node.items():
            if k in ("run", "generate", "script") and isinstance(v, str): scan(label, v)
            else: walk(label, v)
    elif isinstance(node, list):
        for v in node: walk(label, v)
for f in sorted(glob.glob(root + "/.github/workflows/*.y*ml") + glob.glob(root + "/.github/actions/*/action.y*ml")):
    try: doc = yaml.safe_load(open(f))
    except Exception as e: bad.append(f + ": unparsable: " + str(e)); continue
    walk(f.split("/")[-1], doc)
print(seen)
for b in bad: print("BAD " + b)
'
# #4518: the guard parses workflow YAML with PyYAML. A runner without it must FAIL CLOSED naming the module, not
# surface as `seen=` plus a wall of "mutation NOT caught" rows (gate.yml provisions pyyaml==6.0.2).
if ! yaml_err="$(python3 -c "import yaml" 2>&1)"; then
  bad "17: python3 cannot import the yaml module (PyYAML); install pyyaml==6.0.2 (gate.yml provisions it); the standalone-fetch guard and its mutations cannot run: $(tail -n 1 <<<"$yaml_err")"
else
guard_out="$(python3 -c "$GUARD_PY" "$REPO_ROOT")"
guard_seen="$(head -1 <<<"$guard_out")"; guard_bad="$(grep '^BAD ' <<<"$guard_out" || true)"
[ -z "$guard_bad" ] && [ "${guard_seen:-0}" -ge 17 ] && ok "every fetch-datasets.sh invocation ($guard_seen) is a standalone command: no shell-level if/case/loop/||/&& guard" \
  || bad "a guarded or unparsed fetch invocation (seen=$guard_seen): $guard_bad"
# mutation: reintroduce `[ -f x ] || bash ...` / `if ...; then` / `&&` and require each to be caught
MUT="$T/wf-mut"; rm -rf "$MUT"; mkdir -p "$MUT/.github"; cp -R "$REPO_ROOT/.github/workflows" "$MUT/.github/workflows"; cp -R "$REPO_ROOT/.github/actions" "$MUT/.github/actions"
for shape in '[ -f test-data/datasets/.dataset-pin ] || bash ./test-data/scripts/fetch-datasets.sh' 'if [ ! -d test-data/datasets ]; then bash ./test-data/scripts/fetch-datasets.sh; fi' 'test -f x && true && bash ./test-data/scripts/fetch-datasets.sh' 'bash ./test-data/scripts/fetch-datasets.sh || true'; do
  cp "$REPO_ROOT/.github/workflows/observability-gate.yml" "$MUT/.github/workflows/observability-gate.yml"
  python3 - "$MUT/.github/workflows/observability-gate.yml" "$shape" <<'PY'
import sys
p, shape = sys.argv[1], sys.argv[2]
s = open(p).read()
old = "run: bash ./test-data/scripts/fetch-datasets.sh"
assert old in s
open(p, "w").write(s.replace(old, "run: |\n          " + shape.replace("; ", ";\n          "), 1))
PY
  mut_out="$(python3 -c "$GUARD_PY" "$MUT")"
  case "$mut_out" in *"BAD observability-gate.yml"*) ok "mutation caught: $shape" ;; *) bad "mutation NOT caught: $shape => $mut_out" ;; esac
done
fi

# 18. roborev job 208 (2): write_bundle_manifest must not leave a partial manifest when a stage fails. A failing `sort -u`
#     stub on a COLD run: no manifest and no temp file remain; CI refuses (exit 1), a local run continues with a note.
R9="$T/r9"; make_repo "$R9"
REAL_SORT="$(command -v sort)"
printf '#!/bin/sh\ncase " $* " in *" -u "*) echo "sort: stub failure" >&2; exit 1 ;; esac\nexec "%s" "$@"\n' "$REAL_SORT" > "$BIN/sort"; chmod +x "$BIN/sort"
run_fetch "$R9" CI=true
rm -f "$BIN/sort"
{ [ "$RC" != 0 ] && [ ! -e "$R9/$DS/.dataset-manifest" ] && [ -z "$(find "$R9/$DS" -maxdepth 1 -name '.dataset-manifest*' 2>/dev/null)" ]; } \
  && ok "a failing manifest stage: CI refuses (rc $RC), no partial manifest and no temp file left" || bad "partial manifest after a failing stage: rc=$RC $(find "$R9/$DS" -maxdepth 1 -name '.dataset-manifest*' | tr '\n' ' ') $OUT"
R10="$T/r10"; make_repo "$R10"
printf '#!/bin/sh\ncase " $* " in *" -u "*) echo "sort: stub failure" >&2; exit 1 ;; esac\nexec "%s" "$@"\n' "$REAL_SORT" > "$BIN/sort"; chmod +x "$BIN/sort"
run_fetch "$R10"
rm -f "$BIN/sort"
{ [ "$RC" = 0 ] && [ ! -e "$R10/$DS/.dataset-manifest" ] && case "$OUT" in *"could not write"*) true ;; *) false ;; esac; } \
  && ok "a failing manifest stage, local: warned, no manifest, run continues" || bad "local failing-stage run: rc=$RC $OUT"
overlay_cache "$R10"
run_fetch "$R10" CI=true
case "$OUT" in *"forcing a COLD restore"*) ok "a cache with no manifest is never classified from a partial one (forces a cold restore)" ;; *) bad "no cold restore after a missing manifest: $OUT" ;; esac

# 19. roborev job 210 (1): a Windows autocrlf checkout turns the committed twin list into CRLF; exact line compares
#     (the sha header, every path) must still hold. And .gitattributes pins the file to LF.
R11="$T/r11"; make_repo "$R11"; run_fetch "$R11"; pr_changes "$R11"
overlay_cache "$R11"; reset_extra "$R11"
sed 's/$/\r/' "$R11/test-data/dataset-bundle-tracked.txt" > "$T/crlf.txt"; cp "$T/crlf.txt" "$R11/test-data/dataset-bundle-tracked.txt"
[ "$(head -c 200 "$R11/test-data/dataset-bundle-tracked.txt" | tr -cd '\r' | wc -c | tr -d ' ')" -gt 0 ] && ok "setup: the twin list is CRLF" || bad "setup: not CRLF"
run_fetch "$R11" CI=true
{ [ "$RC" = 0 ] && ! has "$R11" removed-Data.db.jsonl && ! has "$R11" old-name-Data.db.jsonl && has "$R11" bundle-only.db; } \
  && ok "a CRLF twin list still classifies exactly (header sha and every path match)" || bad "CRLF twin list broke the classifier ($RC): $(tail -3 <<<"$OUT" | tr "\n" " ")"
grep -qx 'test-data/dataset-bundle-tracked.txt text eol=lf' "$REPO_ROOT/.gitattributes" && ok ".gitattributes forces LF for test-data/dataset-bundle-tracked.txt" || bad ".gitattributes does not pin the twin list to LF"

# 20. roborev job 210 (2): the dataset path is interpolated into the prefix strip; regex metacharacters in it
#     ('.', '[', '*', '|') must be literal. Classify directly against a root whose REL carries them all.
MR="$T/meta/repo"; MREL='we.ird[x]*|y/datasets'; MD="$MR/$MREL"
mkdir -p "$MD/goldens" "$MR/test-data"
printf 'k\n' > "$MD/goldens/keep.jsonl"; printf 'g\n' > "$MD/goldens/gone.jsonl"
printf 'goldens/keep.jsonl\ngoldens/gone.jsonl\n' > "$MD/.dataset-manifest"
printf '# sha256=%s\ngoldens/keep.jsonl\ngoldens/gone.jsonl\n' "$SHA" > "$MR/test-data/dataset-bundle-tracked.txt"
printf '%s\0' "$MREL/goldens/keep.jsonl" > "$T/meta/list0"
META_OUT=$( DATASET_ROOT="$MD" TRACKED_GUARD_STATE=in-repo TRACKED_GUARD_LIST="$T/meta/list0" TRACKED_GUARD_REL="$MREL" TRACKED_GUARD_REPO="$MR" SHA256_EXPECTED="$SHA" \
  bash -c 'set -e; . "$0"; _dataset_stale_lists; echo "TWINS=[$STALE_TWINS]"; echo "EXTRAS=[$STALE_EXTRAS]"' "$REPO_ROOT/test-data/scripts/lib/dataset-warm-restore.sh" 2>&1 )
case "$META_OUT" in
  *"TWINS=[goldens/gone.jsonl]"*"EXTRAS=[]"*) ok "a dataset path with . [ * | is stripped literally: only the deleted file is a twin" ;;
  *) bad "metacharacter dataset path misclassified: $META_OUT" ;;
esac

echo "test_fetch_datasets_warm_overlay: pass=$PASS fail=$FAIL"
[ "$FAIL" = 0 ]
