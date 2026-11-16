#!/usr/bin/env bash
# Self-test for issue #4583: generate_audit_corruption_fixtures.py in REGENERATION mode.
#
# The nightly Parity Regen Matrix re-derives clean sources from a live Cassandra, which is
# not byte-deterministic (Statistics.db / BTI / Summary.db). generate-corruption-corpus.sh
# already treats that as a ::notice:: for every older fixture (MODE 2, #1294); the six
# audit fixtures (#4195) must follow the same contract while the committed-fixture verify
# path (--verify-only, MODE 1) keeps the pinned-sha check.
#
# Pinned here, against the COMMITTED clean sources (test_comp/lz4_table, test_da/ttl_table,
# test_da/wide_table) plus a tiny synthetic BIG Index.db/Summary.db (those two are not
# committed):
#   1. baseline regeneration, then --verify-only against the manifest it wrote: PASS
#   2. clean sources DRIFT (non-deterministic siblings and mutated components):
#        regeneration       -> exit 0, ::notice:: lines, new shas rebound in the manifest
#        --verify-only      -> FATAL on the pinned sha (the MODE 1 contract is kept)
#   4. fail-closed pins (#4583 review): with --pinned-manifest supplied, a missing or unparsable
#      file, a fixture with no entry, a malformed sha (wrong length / non-hex) and a missing sha
#      field each fail with a named cause in --verify-only; regeneration refuses a malformed or
#      missing pin too; --verify-only without --pinned-manifest is a usage error.
#   3. a source that breaks a premise still FAILS regeneration with a premise message
#      (regeneration tolerates sha drift, never a broken premise)
# Hermetic: python3 only, no datasets, no network, no docker.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GEN="$ROOT/test-data/scripts/generate_audit_corruption_fixtures.py"
SRC="$ROOT/test-data/datasets/sstables"
command -v python3 >/dev/null 2>&1 || { echo "SKIP: python3 missing"; exit 0; }
for d in test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77 test_da/ttl_table-de3b579064e711f19ad401a8c8227b11 test_da/wide_table-9099a7c06c1811f19864870fb8444786; do
  [ -d "$SRC/$d" ] || { echo "FAIL: committed clean source $d missing"; exit 1; }
done
W="$(mktemp -d "${TMPDIR:-/tmp}/audit-regen-drift.XXXXXX")" || exit 1
trap 'rm -rf "$W"' EXIT
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); echo "ok   - $*"; }
bad() { FAIL=$((FAIL+1)); echo "FAIL - $*"; }

python3 -I - "$W" "$SRC" <<'PY' || { echo "FAIL: scratch tree build"; exit 1; }
import os, shutil, struct, sys
w, src = sys.argv[1], sys.argv[2]
r = os.path.join(w, "roots")
for d in ("test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77",
          "test_da/ttl_table-de3b579064e711f19ad401a8c8227b11",
          "test_da/wide_table-9099a7c06c1811f19864870fb8444786"):
    shutil.copytree(os.path.join(src, d), os.path.join(r, d),
                    ignore=shutil.ignore_patterns("*.jsonl", "*.txt"))
# Synthetic BIG nb Index.db (6 entries, 1-byte offset VInts, promoted length 0) + Summary.db.
t = os.path.join(r, "test_basic", "simple_table-00000000000000000000000000000000")
os.makedirs(t)
idx, starts = b"", []
for n, off in enumerate((0, 10, 20, 30, 40, 50)):
    starts.append(len(idx))
    key = b"key%d" % n
    idx += struct.pack(">H", len(key)) + key + bytes([off]) + b"\x00"
open(os.path.join(t, "nb-1-big-Index.db"), "wb").write(idx)
open(os.path.join(t, "nb-1-big-Data.db"), "wb").write(b"\x00" * 64)
samples = [(b"key0", starts[0]), (b"key2", starts[2]), (b"key4", starts[4])]
size = len(samples)
offs, pos = [], 4 * size
body = b""
for k, p in samples:
    offs.append(pos)
    body += k + struct.pack("<q", p)
    pos += len(k) + 8
summ = struct.pack(">iiqii", 128, size, 4 * size + len(body), 128, size)
summ += struct.pack("<%di" % size, *offs) + body
open(os.path.join(t, "nb-1-big-Summary.db"), "wb").write(summ)
PY

run_gen() {  # run_gen <corrupt-dir> <roots> [extra args...] ; stdout->$W/out, stderr->$W/err
  local cd="$1" rt="$2"; shift 2
  python3 -I "$GEN" --corrupt-dir "$cd" --root "$rt" "$@" >"$W/out" 2>"$W/err"
}

# 1. Baseline.
run_gen "$W/c1" "$W/roots"; rc=$?
if [ "$rc" = 0 ] && [ "$(grep -c '^  - name: ' "$W/out")" = 8 ]; then
  ok "baseline regeneration emits all 8 audit fixtures"
else
  bad "baseline regeneration rc=$rc: $(tail -3 "$W/err" | tr '\n' ' ')"
fi
grep -q '::notice::' "$W/err" && bad "baseline regeneration must not emit drift notices"
{ echo "fixtures:"; cat "$W/out"; } >"$W/pinned.yml"
cp "$W/out" "$W/out.baseline"
run_gen "$W/c1" "$W/roots" --verify-only --pinned-manifest "$W/pinned.yml"; rc=$?
if [ "$rc" = 0 ]; then ok "--verify-only against the pinned manifest passes on the byte-stable source"
else bad "baseline --verify-only rc=$rc: $(tail -2 "$W/err" | tr '\n' ' ')"; fi

# 2. Drift the clean sources, as a live Cassandra re-derivation does.
python3 -I - "$W/roots" <<'PY' || { echo "FAIL: drift injection"; exit 1; }
import glob, os, struct, sys
r = sys.argv[1]
def one(pat):
    m = glob.glob(os.path.join(r, pat)); assert len(m) == 1, pat; return m[0]
def flip(path, at, xor=0x01):
    b = bytearray(open(path, "rb").read()); b[at] ^= xor; open(path, "wb").write(bytes(b))
# non-deterministic SIBLING of a mutated component (Summary.db next to the lz4 CompressionInfo.db)
flip(one("test_comp/lz4_table-*/nb-1-big-Summary.db"), 30)
# Data.db of a BTI table (sibling of Partitions.db): grows, still shorter than DataOffset(127)
d = one("test_da/ttl_table-*/da-2-bti-Data.db"); open(d, "ab").write(b"\x00" * 3)
# mutated components themselves drift (a different but still valid clean source):
f = one("test_da/wide_table-*/da-2-bti-Filter.db")           # an extra set bit in the bit array
b = bytearray(open(f, "rb").read()); b[-1] |= 0x01; open(f, "wb").write(bytes(b))
s = one("test_comp/lz4_table-*/nb-1-big-Statistics.db")       # a non-STATS component byte (STATS CRC untouched)
b = bytearray(open(s, "rb").read()); n = struct.unpack(">i", b[:4])[0]
toc = [struct.unpack(">ii", b[8 + 8 * k:16 + 8 * k]) for k in range(n)]
other = [t for t in toc if t[0] != 2][0]; b[other[1]] ^= 0x01; open(s, "wb").write(bytes(b))
ix = one("test_basic/simple_table-*/nb-1-big-Data.db"); open(ix, "ab").write(b"\x01")   # sibling of Index.db
PY

run_gen "$W/c2" "$W/roots" --pinned-manifest "$W/pinned.yml"; rc=$?
if [ "$rc" = 0 ]; then ok "regeneration on drifted clean sources exits 0"
else bad "regeneration on drifted sources rc=$rc: $(tail -3 "$W/err" | tr '\n' ' ')"; fi
nn="$(grep -c '^::notice::' "$W/err")"
if [ "$nn" -ge 2 ]; then ok "sha drift is reported as ::notice:: ($nn lines)"; else bad "expected >=2 ::notice:: lines, got $nn"; fi
if grep -q '^::notice::filter_db_bit_flip_bti: corrupted_sha256 drifted' "$W/err" \
  && grep -q '^::notice::statistics_partition_count_mismatch: corrupted_sha256 drifted' "$W/err"; then
  ok "notices name the fixtures whose mutated component drifted"
else bad "missing per-fixture notices: $(head -c 400 "$W/err")"; fi
if ! cmp -s "$W/out" "$W/out.baseline"; then ok "the new shas are rebound in the emitted manifest entries"
else bad "emitted manifest did not change under drift"; fi
run_gen "$W/c2" "$W/roots" --verify-only --pinned-manifest "$W/pinned.yml"; rc=$?
if [ "$rc" != 0 ] && grep -q 'drifted from its pinned value' "$W/err"; then
  ok "--verify-only keeps the pinned-sha check: drift is FATAL"
else bad "--verify-only must fail on pinned drift (rc=$rc)"; fi

# 3. A broken premise still fails regeneration.
python3 -I - "$W/roots" <<'PY'
import glob, sys
p = glob.glob(sys.argv[1] + "/test_da/ttl_table-*/da-2-bti-Partitions.db")[0]
b = bytearray(open(p, "rb").read()); b[0] = 0x00; open(p, "wb").write(bytes(b))
PY
run_gen "$W/c3" "$W/roots" --pinned-manifest "$W/pinned.yml"; rc=$?
if [ "$rc" != 0 ] && grep -q 'bti_data_offset_out_of_bounds: premise failed' "$W/err"; then
  ok "a broken premise fails regeneration even though sha drift is tolerated"
else bad "premise break must fail regeneration (rc=$rc): $(tail -2 "$W/err" | tr '\n' ' ')"; fi

# 4. Fail-closed pins. Use pristine (undrifted) sources so only the pin defect can fail.
rm -rf "$W/roots4"; mkdir "$W/roots4"
python3 -I - "$W/roots4" "$SRC" <<'PY'
import os, shutil, sys
r, src = sys.argv[1:3]
for d in ("test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77",
          "test_da/ttl_table-de3b579064e711f19ad401a8c8227b11",
          "test_da/wide_table-9099a7c06c1811f19864870fb8444786"):
    shutil.copytree(os.path.join(src, d), os.path.join(r, d), ignore=shutil.ignore_patterns("*.jsonl", "*.txt"))
PY
cp -r "$W/roots/test_basic" "$W/roots4/test_basic"
# undo case 2's appended byte on the synthetic Data.db (sibling of Index.db)
python3 -I - "$W/roots4" <<'PY'
import glob, os, sys
p = glob.glob(os.path.join(sys.argv[1], "test_basic/simple_table-*/nb-1-big-Data.db"))[0]
open(p, "wb").write(b"\x00" * 64)
PY
run_gen "$W/c4" "$W/roots4"; [ $? = 0 ] || bad "case 4 regeneration baseline failed: $(tail -2 "$W/err" | tr '\n' ' ')"
{ echo "fixtures:"; cat "$W/out"; } >"$W/pin4.yml"
run_gen "$W/c4" "$W/roots4" --verify-only --pinned-manifest "$W/pin4.yml" \
  || bad "case 4 verify baseline failed: $(tail -2 "$W/err" | tr '\n' ' ')"

expect_fail() {  # expect_fail <label> <cause-regex> <args...>
  local label="$1" cause="$2"; shift 2
  run_gen "$W/c4" "$W/roots4" "$@"; local rc=$?
  if [ "$rc" != 0 ] && grep -Eq "$cause" "$W/err"; then ok "$label"
  else bad "$label (rc=$rc): $(tail -2 "$W/err" | tr '\n' ' ')"; fi
}
ZS="$(printf 'z%.0s' $(seq 64))"
# 4a. missing manifest file
expect_fail "--verify-only: a missing pinned manifest file fails, naming it unreadable" 'unreadable' \
  --verify-only --pinned-manifest "$W/does-not-exist.yml"
expect_fail "regeneration: a missing pinned manifest file fails too" 'unreadable' \
  --pinned-manifest "$W/does-not-exist.yml"
# 4b. --verify-only without --pinned-manifest
expect_fail "--verify-only without --pinned-manifest is a usage error" 'requires --pinned-manifest' --verify-only
# 4c. unparsable
echo "garbage" >"$W/pin-empty.yml"
expect_fail "--verify-only: an unparsable manifest fails (no entries found)" 'no manifest entries' \
  --verify-only --pinned-manifest "$W/pin-empty.yml"
# 4d. fixture missing from the manifest
python3 -I - "$W/pin4.yml" "$W/pin-noentry.yml" <<'PY'
import re, sys
t = open(sys.argv[1]).read()
blocks = re.split(r"(?=\n  - name: )", t)
keep = [b for b in blocks if "name: filter_db_bit_flip_bti" not in b]
assert len(keep) == len(blocks) - 1
open(sys.argv[2], "w").write("".join(keep))
PY
expect_fail "--verify-only: a fixture with no manifest entry fails, naming it" 'filter_db_bit_flip_bti: no entry in the pinned manifest' \
  --verify-only --pinned-manifest "$W/pin-noentry.yml"
# 4e. malformed sha: wrong length, then non-hex
sed 's/^\(    corrupted_sha256: "\)[0-9a-f]*\(".*\)$/\1abc123\2/' "$W/pin4.yml" >"$W/pin-short.yml"
expect_fail "--verify-only: a wrong-length sha fails, naming it malformed" 'corrupted_sha256 is malformed' \
  --verify-only --pinned-manifest "$W/pin-short.yml"
sed "s/^\(    original_sha256: \"\)[0-9a-f]*\(\".*\)\$/\1$ZS\2/" "$W/pin4.yml" >"$W/pin-nonhex.yml"
expect_fail "--verify-only: a non-hex sha fails, naming it malformed" 'original_sha256 is malformed' \
  --verify-only --pinned-manifest "$W/pin-nonhex.yml"
expect_fail "regeneration refuses a malformed sha pin" 'is malformed' --pinned-manifest "$W/pin-short.yml"
# 4f. missing sha field
sed '/^    corrupted_sha256: /d' "$W/pin4.yml" >"$W/pin-nofield.yml"
expect_fail "--verify-only: a missing sha field fails, naming it" 'is missing corrupted_sha256' \
  --verify-only --pinned-manifest "$W/pin-nofield.yml"
expect_fail "regeneration refuses a pin with a missing sha field" 'is missing corrupted_sha256' \
  --pinned-manifest "$W/pin-nofield.yml"

echo "audit-regen-drift: $PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
