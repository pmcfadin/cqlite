#!/usr/bin/env bash
# generate-corruption-corpus.sh — Deterministic corrupted-component fixture
# corpus (epic #970, issue #999).
#
# Builds keyspace-like directory `test_comp_corrupt/` holding ONE corrupted
# variant per intentional single-component corruption the CQLite
# compression / robustness epic must DETECT and reject byte-for-byte the same
# way Apache Cassandra's `nodetool verify` / read path does.
#
# Unlike generate-compression-parity.sh / generate-tombstone-parity.sh this
# generator needs NO Cassandra container and NO Docker. It is a pure,
# offline, deterministic transform:
#
#     copy clean source component dir  -->  apply exactly ONE fixed-offset
#     byte mutation  -->  emit per-fixture metadata.
#
# Because every mutation is a FIXED (offset, original_byte, mutated_byte)
# tuple applied to a committed clean fixture, re-running this script produces
# BYTE-IDENTICAL corrupted files every time (no randomness, no timestamps, no
# UUID churn). CI consumes the DESCRIBED corruptions from
# corruption-manifest.yml; it MUST NOT mutate bytes at test time.
#
# =====================================================================
# DETERMINISM CONTRACT
# =====================================================================
#   * NO randomness. The mutation table below is fixed and audited.
#   * Each corrupted fixture differs from its clean source at EXACTLY ONE
#     byte offset (truncations excepted: a truncation removes a fixed suffix
#     and the verifier proves "clean prefix identical, tail removed").
#   * Clean sources are NEVER modified. We only ever read them.
#   * Corrupted *.db binaries are gitignored (like all *.db); only the text
#     artifacts (corruption-manifest.yml, *.sha256.txt, verification report,
#     this script) are committed and regeneratable.
#
# =====================================================================
# CORRUPTION TABLE  (manifest key -> component -> mutation -> offset)
# =====================================================================
#   data_db_bit_flip            Data.db            single-bit flip      fixed
#   data_db_truncation          Data.db            truncate tail        fixed
#   compression_info_bad_offset CompressionInfo.db chunk-offset MSB set fixed
#   index_db_bit_flip_big       Index.db (nb/BIG)  single-bit flip      fixed
#   bti_partitions_footer_flip  Partitions.db(da)  footer bit flip      fixed
#   bti_rows_truncation         Rows.db (da)       truncate tail        fixed
#   statistics_db_header_damage Statistics.db      header count damage  fixed
#   summary_db_truncation       Summary.db         truncate tail        fixed
#   toc_missing_component       TOC.txt            drop one line        fixed
#   digest_crc32_mismatch       Digest.crc32       flip last digit      fixed
#
# Usage:
#   bash test-data/scripts/generate-corruption-corpus.sh \
#        [--out <datasets-dir>] [--bti-source-root <dir>] [--dry-run] [--verify-only]
#
# Options:
#   --out <dir>              Datasets root (default: test-data/datasets).
#                            Corrupted corpus is written under
#                            <dir>/sstables/test_comp_corrupt/.
#   --bti-source-root <dir>  Extra datasets root to search for clean BTI (`da`)
#                            sources (Partitions.db / Rows.db). Useful when the
#                            current worktree only has the nb/test_comp binaries
#                            fetched but the BTI binaries live in a sibling repo.
#   --dry-run                Print the plan without writing any files.
#   --verify-only            Skip (re)generation; just verify + report against
#                            whatever is already on disk.
#
# Backs: epic #970 (issue #999)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
OUT_DIR="${OUT_DIR:-$ROOT/datasets}"
BTI_SOURCE_ROOT="${BTI_SOURCE_ROOT:-}"
DRY_RUN="${DRY_RUN:-0}"
VERIFY_ONLY="${VERIFY_ONLY:-0}"

CORRUPT_KS="test_comp_corrupt"
CLEAN_KS="test_comp"

# ---------------------------------------------------------------------------
# Parse CLI flags
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)             OUT_DIR="$2"; shift 2 ;;
    --bti-source-root) BTI_SOURCE_ROOT="$2"; shift 2 ;;
    --dry-run)         DRY_RUN=1; shift ;;
    --verify-only)     VERIFY_ONLY=1; shift ;;
    *) echo "[corrupt] Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then OUT_DIR="$PWD/$OUT_DIR"; fi
OUT_DIR="${OUT_DIR%/}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[corrupt] $*"; }
fail() { echo "[corrupt][ERROR] $*" >&2; exit 1; }

# Pick a SHA-256 tool (Linux: sha256sum, macOS: shasum -a 256).
if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  fail "Neither sha256sum nor shasum found in PATH."
fi

command -v python3 >/dev/null 2>&1 || fail "python3 is required."

# ---------------------------------------------------------------------------
# Path-safety guard for OUT_DIR (mirrors the other generators).
# ---------------------------------------------------------------------------
if [[ "${#OUT_DIR}" -lt 4 ]]; then fail "OUT_DIR '$OUT_DIR' suspiciously short."; fi
case "$OUT_DIR" in
  /)    fail "Refusing to operate on '/'." ;;
  /tmp) fail "Refusing to use '/tmp' directly." ;;
esac
_under_repo=0; _under_tmp=0
[[ "$OUT_DIR" == "$REPO_ROOT/"* ]] && _under_repo=1
[[ "$OUT_DIR" == /tmp/*          ]] && _under_tmp=1
if [[ "$_under_repo" -eq 0 && "$_under_tmp" -eq 0 ]]; then
  fail "OUT_DIR '$OUT_DIR' is not under the repo root or /tmp/."
fi

SSTABLES_DIR="$OUT_DIR/sstables"
CLEAN_DIR="$SSTABLES_DIR/$CLEAN_KS"
# Corrupt corpus lives OUTSIDE sstables/ so the query engine and fixture-
# discovery walkers never treat intentionally-corrupted data as a valid table.
# The verifier/tests read these by explicit path. (epic #970, issue #999)
CORRUPT_DIR="$OUT_DIR/corruption/$CORRUPT_KS"
MANIFEST="$CORRUPT_DIR/corruption-manifest.yml"
REPORT="$CORRUPT_DIR/verification-report.txt"

# ---------------------------------------------------------------------------
# Resolve clean source component directories (table-UUID dirs change per
# regeneration, so resolve by table-name prefix, not the committed UUID).
# ---------------------------------------------------------------------------
resolve_clean() {
  # $1 = table name prefix (e.g. lz4_table)
  local prefix="$1" d
  d="$(find "$CLEAN_DIR" -maxdepth 1 -type d -name "${prefix}-*" 2>/dev/null | sort | head -1)"
  [[ -n "$d" ]] || return 1
  echo "$d"
}

# Resolve a clean BTI (`da`) source dir that has a non-empty $2 component.
# Searches: --out datasets, $CQLITE_DATASETS_ROOT, --bti-source-root, and a
# sibling main-repo checkout (../cqlite/test-data/datasets).
resolve_bti() {
  # $1 = preferred table prefix (e.g. wide_table); $2 = required non-empty component suffix (Partitions.db / Rows.db / "")
  local prefix="$1" need="$2"
  local roots=()
  roots+=("$SSTABLES_DIR/test_da")
  [[ -n "${CQLITE_DATASETS_ROOT:-}" ]] && roots+=("$CQLITE_DATASETS_ROOT/sstables/test_da")
  [[ -n "$BTI_SOURCE_ROOT" ]] && roots+=("$BTI_SOURCE_ROOT/sstables/test_da")
  roots+=("$REPO_ROOT/../cqlite/test-data/datasets/sstables/test_da")
  local r d comp
  for r in "${roots[@]}"; do
    [[ -d "$r" ]] || continue
    # Prefer the requested prefix, then any da- table with a qualifying component.
    for d in "$r/${prefix}-"* "$r"/*; do
      [[ -d "$d" ]] || continue
      comp="$(find "$d" -maxdepth 1 -type f -name "da-*-${need}" 2>/dev/null | sort | head -1)"
      if [[ -n "$need" && -n "$comp" && -s "$comp" ]]; then
        echo "$d"; return 0
      fi
      if [[ -z "$need" && -n "$comp" ]]; then
        echo "$d"; return 0
      fi
    done
  done
  return 1
}

# ---------------------------------------------------------------------------
# Mutation primitives (python3 backends so behaviour is identical on
# macOS/Linux and the offset arithmetic is unambiguous).
# ---------------------------------------------------------------------------

# Single-byte XOR-bit-flip at a fixed offset. Echoes "orig_hex mut_hex".
apply_bit_flip() {
  # $1 src $2 dst $3 offset $4 bitmask(int)
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import sys, shutil
src, dst, off, mask = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
data = bytearray(open(src, "rb").read())
if off < 0 or off >= len(data):
    sys.exit(f"offset {off} out of range for {src} (len {len(data)})")
orig = data[off]
mut = orig ^ mask
data[off] = mut
open(dst, "wb").write(bytes(data))
print(f"{orig:02x} {mut:02x}")
PY
}

# Set a fixed byte to a fixed value (used for header-count damage / MSB set).
apply_set_byte() {
  # $1 src $2 dst $3 offset $4 newval(int)
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import sys
src, dst, off, newv = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
data = bytearray(open(src, "rb").read())
if off < 0 or off >= len(data):
    sys.exit(f"offset {off} out of range for {src} (len {len(data)})")
orig = data[off]
data[off] = newv & 0xFF
open(dst, "wb").write(bytes(data))
print(f"{orig:02x} {newv & 0xFF:02x}")
PY
}

# Truncate to keep_bytes from the front. Echoes "orig_len kept_len".
apply_truncate() {
  # $1 src $2 dst $3 keep_bytes
  python3 - "$1" "$2" "$3" <<'PY'
import sys
src, dst, keep = sys.argv[1], sys.argv[2], int(sys.argv[3])
data = open(src, "rb").read()
if keep < 0 or keep >= len(data):
    sys.exit(f"keep {keep} not a strict truncation for {src} (len {len(data)})")
open(dst, "wb").write(data[:keep])
print(f"{len(data)} {keep}")
PY
}

# Drop a single matching line from a TOC.txt. Echoes the dropped line.
apply_toc_drop() {
  # $1 src $2 dst $3 line-to-drop
  python3 - "$1" "$2" "$3" <<'PY'
import sys
src, dst, drop = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(src).read().splitlines(keepends=True)
out, dropped = [], None
for ln in lines:
    if dropped is None and ln.rstrip("\r\n") == drop:
        dropped = ln.rstrip("\r\n"); continue
    out.append(ln)
if dropped is None:
    sys.exit(f"component '{drop}' not present in {src}")
open(dst, "w").write("".join(out))
print(dropped)
PY
}

# Flip the last numeric digit of a Digest.crc32 (text decimal). Echoes "orig mut".
apply_digest_mismatch() {
  # $1 src $2 dst
  python3 - "$1" "$2" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
raw = open(src).read()
s = raw.strip()
if not s or not s[-1].isdigit():
    sys.exit(f"unexpected Digest.crc32 content in {src!r}")
last = s[-1]
new_last = "0" if last != "0" else "1"
mut = s[:-1] + new_last
# Preserve any trailing whitespace/newline shape from the original.
tail = raw[len(raw.rstrip()):]
open(dst, "w").write(mut + tail)
print(f"{s} {mut}")
PY
}

# Render a path relative to REPO_ROOT when it lives under it; otherwise tag it
# as an out-of-tree source (keeps the committed manifest machine-independent and
# deterministic). BTI sources resolved from a sibling main-repo checkout are
# recorded by their stable datasets-relative tail so they are reproducible.
rel_path() {
  local p="$1"
  case "$p" in
    "$REPO_ROOT/"*) echo "${p#"$REPO_ROOT"/}" ;;
    *)
      # Keep the stable "<...>/test-data/datasets/sstables/..." tail if present.
      if [[ "$p" == *"/test-data/datasets/sstables/"* ]]; then
        echo "<out-of-tree>/test-data/datasets/sstables/${p#*"/test-data/datasets/sstables/"}"
      else
        echo "$p"
      fi
      ;;
  esac
}

# First byte offset where two files differ (-1 if identical prefix up to min len).
first_diff_offset() {
  python3 - "$1" "$2" <<'PY'
import sys
a = open(sys.argv[1], "rb").read()
b = open(sys.argv[2], "rb").read()
n = min(len(a), len(b))
for i in range(n):
    if a[i] != b[i]:
        print(i); break
else:
    print(-1)
PY
}

# Count of differing byte positions over the overlapping prefix.
count_diff_bytes() {
  python3 - "$1" "$2" <<'PY'
import sys
a = open(sys.argv[1], "rb").read()
b = open(sys.argv[2], "rb").read()
n = min(len(a), len(b))
print(sum(1 for i in range(n) if a[i] != b[i]))
PY
}

# ---------------------------------------------------------------------------
# Resolve clean sources up-front (fail loud if the nb corpus binaries are absent).
# ---------------------------------------------------------------------------
[[ -d "$CLEAN_DIR" ]] || fail "Clean source keyspace not found: $CLEAN_DIR (run generate-compression-parity.sh and fetch-datasets.sh first)."

SRC_LZ4="$(resolve_clean lz4_table || true)"
SRC_UNC="$(resolve_clean uncompressed_table || true)"
[[ -n "$SRC_LZ4" && -f "$SRC_LZ4/nb-1-big-Data.db" ]] || fail "lz4_table clean Data.db binary missing under $CLEAN_DIR (fetch datasets)."
[[ -n "$SRC_UNC" ]] || fail "uncompressed_table clean source missing under $CLEAN_DIR."

# BTI sources (may be absent -> those entries become 'planned').
BTI_PART_SRC="$(resolve_bti wide_table Partitions.db || true)"
BTI_ROWS_SRC="$(resolve_bti wide_table Rows.db || true)"

log "Clean nb source (Data/Index/Stats/Summary/Compression): $SRC_LZ4"
log "BTI Partitions source: ${BTI_PART_SRC:-<none found -> planned>}"
log "BTI Rows source:       ${BTI_ROWS_SRC:-<none found -> planned>}"

# ---------------------------------------------------------------------------
# Fixed mutation table.
#   name|manifest_key|src_dir|component|mutation_type|param1|param2|expected_component|error_class|rationale
# mutation_type:
#   bitflip   param1=offset param2=bitmask
#   setbyte   param1=offset param2=newval
#   truncate  param1=keep_bytes
#   tocdrop   param1=line
#   digest    (no params)
# A blank src_dir => planned (no source available).
# ---------------------------------------------------------------------------

PART_DIR_BTI="${BTI_PART_SRC:-}"
ROWS_DIR_BTI="${BTI_ROWS_SRC:-}"

# Byte offsets justified in the comments below (validated against the committed
# clean fixtures at authoring time):
#   * CompressionInfo.db chunk-offset array starts after the header; for
#     LZ4Compressor (name_len 13) the chunk[1] u64 offset begins at byte 47.
#     Setting its most-significant byte (0x00 -> 0x80) makes the chunk offset
#     ~9.2e18, far past Data.db end -> the reader must reject it.
#   * Statistics.db byte 0 is the high byte of the MetadataSerializer component
#     count (0x00000004). Setting it to 0xFF makes the count ~4.28e9 -> the
#     header parse fails immediately.
#   * Index.db (nb/BIG) byte 7 sits inside the first partition's serialized
#     position/promoted-index region; XOR 0x40 flips a bit there.
#   * BTI Partitions.db footer: the final byte is the low byte of the trailing
#     root/trie footer word; XOR 0x01 corrupts the root pointer.

FIXTURES=()
FIXTURES+=("data_db_bit_flip|cass.corruption.data_db.bit_flip|$SRC_LZ4|nb-1-big-Data.db|bitflip|64|1|Data.db|ChunkDecompressionError/CrcMismatch|Single-bit flip inside the first compressed chunk payload corrupts the LZ4 chunk so decompression / inline CRC check fails.")
FIXTURES+=("data_db_truncation|cass.corruption.data_db.truncation|$SRC_LZ4|nb-1-big-Data.db|truncate|4096||Data.db|ChunkOffsetOutOfBounds/DigestMismatch|Tail truncated so CompressionInfo.db chunk offsets now point past the shortened Data.db (ChunkOffsetOutOfBounds) and the Data.db digest no longer matches (DigestMismatch); the row scan then fails.")
FIXTURES+=("compression_info_bad_offset|cass.corruption.compression_info.bad_offset|$SRC_LZ4|nb-1-big-CompressionInfo.db|setbyte|47|128|CompressionInfo.db|CompressionInfoCorrupt/ChunkOffsetOutOfBounds|chunk[1] offset MSB set -> caught as CompressionInfoCorrupt (parse rejects the out-of-range/non-ascending offset) or ChunkOffsetOutOfBounds (offset-vs-Data.db bounds check), depending on which guard trips first; either way the bad offset is surfaced, never silently read.")
FIXTURES+=("index_db_bit_flip_big|cass.corruption.index_db.bit_flip_big|$SRC_LZ4|nb-1-big-Index.db|bitflip|7|64|Index.db|IndexEntryCorrupt|Single-bit flip in the first Index.db partition entry corrupts the promoted index / position.")
FIXTURES+=("bti_partitions_footer_flip|cass.corruption.bti_partitions_footer_bit_flip|$PART_DIR_BTI|__BTI_PART__|bitflip|__LAST__|1|Partitions.db|BtiRootPointerCorrupt|Footer (root pointer) bit flip in the BTI Partitions.db trie -> root node seek lands at the wrong byte.")
FIXTURES+=("bti_rows_truncation|cass.corruption.bti_rows_truncation|$ROWS_DIR_BTI|__BTI_ROWS__|truncate|256||Rows.db|BtiTrieCorrupt|Rows.db trie truncated mid-node so per-partition row-trie traversal fails (BtiTrieCorrupt).")
FIXTURES+=("statistics_db_header_damage|cass.corruption.statistics_db.header_damage|$SRC_LZ4|nb-1-big-Statistics.db|setbyte|0|255|Statistics.db|StatisticsHeaderCorrupt|MetadataSerializer component-count high byte set to 0xFF -> count ~4.28e9, header parse fails.")
FIXTURES+=("summary_db_truncation|cass.corruption.summary_db_truncation|$SRC_LZ4|nb-1-big-Summary.db|truncate|16||Summary.db|SummaryCorrupt|Summary.db truncated inside the index-samples block so the summary cannot be deserialized (SummaryCorrupt).")
FIXTURES+=("toc_missing_component|cass.corruption.toc_missing_component|$SRC_LZ4|nb-1-big-TOC.txt|tocdrop|Statistics.db||TOC.txt|MissingComponent|TOC.txt no longer lists Statistics.db -> component discovery reports a missing mandatory component.")
FIXTURES+=("digest_crc32_mismatch|cass.corruption.digest_crc32_mismatch|$SRC_LZ4|nb-1-big-Digest.crc32|digest|||Digest.crc32|DigestMismatch|Recorded whole-file CRC no longer matches Data.db -> digest verification fails.")

# ---------------------------------------------------------------------------
# Dry-run plan
# ---------------------------------------------------------------------------
if [[ "$DRY_RUN" -eq 1 ]]; then
  log "DRY RUN — planned corrupted corpus under $CORRUPT_DIR"
  for spec in "${FIXTURES[@]}"; do
    IFS='|' read -r name key src comp mtype p1 p2 exp errc rationale <<<"$spec"
    if [[ -z "$src" ]]; then
      echo "  [PLANNED-NO-SOURCE] $name ($key) -> $comp $mtype (no clean source available)"
    else
      echo "  [PLAN] $name ($key): copy $(basename "$src") -> $comp / $mtype($p1${p2:+,$p2})  expect:$exp[$errc]"
    fi
  done
  exit 0
fi

# ---------------------------------------------------------------------------
# (Re)generate corpus
# ---------------------------------------------------------------------------
if [[ "$VERIFY_ONLY" -eq 0 ]]; then
  rm -rf "$CORRUPT_DIR"
  mkdir -p "$CORRUPT_DIR"
fi
[[ -d "$CORRUPT_DIR" ]] || fail "Corrupt corpus dir missing: $CORRUPT_DIR (run without --verify-only first)."

# Manifest + report accumulate as we go.
MANIFEST_TMP="$(mktemp)"
REPORT_TMP="$(mktemp)"
trap 'rm -f "$MANIFEST_TMP" "$REPORT_TMP"' EXIT

{
  echo "# corruption-manifest.yml — deterministic corrupted-component corpus"
  echo "# Generated by test-data/scripts/generate-corruption-corpus.sh (epic #970, issue #999)."
  echo "# Each entry corrupts EXACTLY ONE component of a clean test_comp / test_da fixture."
  echo "# Corrupted *.db binaries are gitignored and regeneratable byte-for-byte from this manifest."
  echo "schema_version: 1"
  echo "epic: 970"
  echo "issue: 999"
  echo "clean_keyspace: $CLEAN_KS"
  echo "corrupt_keyspace: $CORRUPT_KS"
  echo "fixtures:"
} >"$MANIFEST_TMP"

{
  echo "================================================================"
  echo " CORRUPTION CORPUS VERIFICATION REPORT (epic #970, issue #999)"
  echo "================================================================"
  echo "corrupt corpus: $(rel_path "$CORRUPT_DIR")"
  echo "clean source  : $(rel_path "$CLEAN_DIR")"
  printf "\n%-30s %-20s %-12s %-9s %s\n" "FIXTURE" "COMPONENT" "MUTATION" "OFFSET" "STATUS"
  echo "----------------------------------------------------------------------------------------"
} >"$REPORT_TMP"

PLANNED=()
GENERATED=()

emit_manifest_entry() {
  # All args are pre-escaped scalars.
  local name="$1" key="$2" clean_src="$3" component="$4" mtype="$5" \
        offset="$6" orig_hex="$7" mut_hex="$8" orig_sha="$9" corr_sha="${10}" \
        orig_len="${11}" corr_len="${12}" exp="${13}" errc="${14}" rationale="${15}" status="${16}" \
        clean_table="${17:-}"
  local corr_rel="corruption/$CORRUPT_KS/$name/$component"
  {
    echo "  - name: $name"
    echo "    manifest_key: $key"
    echo "    status: $status"
    echo "    component: $component"
    echo "    clean_source_table: \"$clean_table\""
    echo "    clean_source_path: \"$clean_src\""
    echo "    corrupted_path: \"$corr_rel\""
    echo "    mutation_type: $mtype"
    echo "    byte_offset: $offset"
    echo "    original_bytes_hex: \"$orig_hex\""
    echo "    mutated_bytes_hex: \"$mut_hex\""
    echo "    original_sha256: \"$orig_sha\""
    echo "    corrupted_sha256: \"$corr_sha\""
    echo "    original_size_bytes: $orig_len"
    echo "    corrupted_size_bytes: $corr_len"
    echo "    expected_failing_component: $exp"
    echo "    expected_error_class: $errc"
    echo "    rationale: \"$rationale\""
  } >>"$MANIFEST_TMP"
}

for spec in "${FIXTURES[@]}"; do
  IFS='|' read -r name key src comp mtype p1 p2 exp errc rationale <<<"$spec"

  # --- planned (no source) ---------------------------------------------------
  if [[ -z "$src" ]]; then
    PLANNED+=("$name")
    emit_manifest_entry "$name" "$key" "(none — clean BTI source not available in this checkout)" \
      "$comp" "$mtype" "n/a" "" "" "" "" "0" "0" "$exp" "$errc" "$rationale" "planned" "(unresolved)"
    printf "%-30s %-20s %-12s %-9s %s\n" "$name" "$comp" "$mtype" "-" "PLANNED (no clean source)" >>"$REPORT_TMP"
    continue
  fi

  # --- resolve concrete component file + dest dir ---------------------------
  dest="$CORRUPT_DIR/$name"
  case "$comp" in
    __BTI_PART__) comp="$(basename "$(find "$src" -maxdepth 1 -name 'da-*-Partitions.db' | sort | head -1)")" ;;
    __BTI_ROWS__) comp="$(basename "$(find "$src" -maxdepth 1 -name 'da-*-Rows.db'       | sort | head -1)")" ;;
  esac
  clean_file="$src/$comp"
  [[ -f "$clean_file" ]] || fail "$name: clean component not found: $clean_file"

  if [[ "$VERIFY_ONLY" -eq 0 ]]; then
    rm -rf "$dest"; mkdir -p "$dest"
    # Copy the WHOLE clean component dir so the corrupted fixture is a complete,
    # loadable SSTable directory (only ONE component differs from clean).
    cp -R "$src/." "$dest/"
    # Strip regeneratable text sidecars (sstabledump JSONL, sstablemetadata /
    # CompressionInfo decode .txt). They are NOT SSTable components and would
    # otherwise be committed as duplicate noise. We keep only loadable SSTable
    # components: *.db binaries (gitignored) plus the text components TOC.txt,
    # Digest.crc32 and CRC.db. The corrupted TOC.txt / Digest.crc32 fixtures ARE
    # the corruption artifact and are intentionally committed.
    find "$dest" -maxdepth 1 -type f \
      \( -name '*.db.jsonl' -o -name '*.db.txt' -o -name '.DS_Store' -o -name '._*' \) \
      -delete 2>/dev/null || true
  fi

  corrupt_file="$dest/$comp"
  [[ -f "$corrupt_file" ]] || fail "$name: copied component missing: $corrupt_file"

  # Stable, UUID-independent identifier for the clean source table.
  clean_table="$(basename "$(dirname "$clean_file")")"
  clean_table="${clean_table%%-*}"

  orig_sha="$(sha256 "$clean_file")"
  orig_len="$(python3 -c "import os,sys;print(os.path.getsize(sys.argv[1]))" "$clean_file")"

  # Derive the deterministic mutation offset from the fixed params FIRST so that
  # --verify-only (which does not re-apply the mutation) still validates the
  # diff lands at the expected site.
  offset="n/a"; orig_hex=""; mut_hex=""
  case "$mtype" in
    bitflip)
      offset="$p1"
      [[ "$offset" == "__LAST__" ]] && offset="$((orig_len - 1))"
      ;;
    setbyte)  offset="$p1" ;;
    truncate) offset="$p1" ;;
    tocdrop)  offset="line" ;;
    digest)   offset="text" ;;
    *) fail "$name: unknown mutation type '$mtype'" ;;
  esac

  if [[ "$VERIFY_ONLY" -eq 0 ]]; then
    case "$mtype" in
      bitflip)
        read -r orig_hex mut_hex < <(apply_bit_flip "$clean_file" "$corrupt_file" "$offset" "$p2")
        ;;
      setbyte)
        read -r orig_hex mut_hex < <(apply_set_byte "$clean_file" "$corrupt_file" "$offset" "$p2")
        ;;
      truncate)
        read -r o_len k_len < <(apply_truncate "$clean_file" "$corrupt_file" "$p1")
        orig_hex="len=$o_len"; mut_hex="len=$k_len"
        ;;
      tocdrop)
        dropped="$(apply_toc_drop "$clean_file" "$corrupt_file" "$p1")"
        orig_hex="line:$dropped"; mut_hex="removed"
        ;;
      digest)
        read -r d_orig d_mut < <(apply_digest_mismatch "$clean_file" "$corrupt_file")
        orig_hex="$d_orig"; mut_hex="$d_mut"
        ;;
    esac
  else
    # verify-only: recompute hex evidence from the bytes already on disk.
    case "$mtype" in
      bitflip|setbyte)
        orig_hex="$(python3 -c "import sys;print('%02x'%open(sys.argv[1],'rb').read()[int(sys.argv[2])])" "$clean_file" "$offset")"
        mut_hex="$(python3 -c "import sys;print('%02x'%open(sys.argv[1],'rb').read()[int(sys.argv[2])])" "$corrupt_file" "$offset")"
        ;;
      truncate) orig_hex="len=$orig_len"; mut_hex="len=$(python3 -c "import os,sys;print(os.path.getsize(sys.argv[1]))" "$corrupt_file")" ;;
      tocdrop)  orig_hex="line:$p1"; mut_hex="removed" ;;
      digest)   orig_hex="$(tr -d '[:space:]' <"$clean_file")"; mut_hex="$(tr -d '[:space:]' <"$corrupt_file")" ;;
    esac
  fi

  corr_sha="$(sha256 "$corrupt_file")"
  corr_len="$(python3 -c "import os,sys;print(os.path.getsize(sys.argv[1]))" "$corrupt_file")"

  # ---- single-mutation proof ----------------------------------------------
  status="OK"
  if [[ "$mtype" == "truncate" ]]; then
    # Strict suffix removal: corrupted must be a strict prefix of clean.
    pre_ok="$(python3 - "$clean_file" "$corrupt_file" <<'PY'
import sys
a=open(sys.argv[1],"rb").read(); b=open(sys.argv[2],"rb").read()
print("yes" if (len(b)<len(a) and a[:len(b)]==b) else "no")
PY
)"
    [[ "$pre_ok" == "yes" ]] || status="FAIL(not-strict-prefix)"
  elif [[ "$mtype" == "tocdrop" || "$mtype" == "digest" ]]; then
    # Text mutations: prove corrupted != clean and that the recorded SHA changed.
    [[ "$corr_sha" != "$orig_sha" ]] || status="FAIL(no-change)"
  else
    # Byte mutations: EXACTLY ONE differing byte and the SHA changed.
    ndiff="$(count_diff_bytes "$clean_file" "$corrupt_file")"
    fdiff="$(first_diff_offset "$clean_file" "$corrupt_file")"
    if [[ "$ndiff" -ne 1 ]]; then status="FAIL(diff-bytes=$ndiff)"; fi
    if [[ "$fdiff" != "$offset" ]]; then status="FAIL(diff-at=$fdiff,expected=$offset)"; fi
    if [[ "$corr_sha" == "$orig_sha" ]]; then status="FAIL(sha-unchanged)"; fi
  fi

  # Also: every OTHER component in the corrupted dir must be byte-identical to
  # clean (only the named component changed).
  other_diffs=0
  while IFS= read -r -d '' cf; do
    rel="${cf#"$dest"/}"
    [[ "$rel" == "$comp" ]] && continue
    [[ -f "$src/$rel" ]] || { other_diffs=$((other_diffs+1)); continue; }
    if [[ "$(sha256 "$cf")" != "$(sha256 "$src/$rel")" ]]; then
      other_diffs=$((other_diffs+1))
    fi
  done < <(find "$dest" -maxdepth 1 -type f -print0)
  if [[ "$other_diffs" -ne 0 ]]; then status="$status FAIL(other-components-changed=$other_diffs)"; fi

  GENERATED+=("$name")
  emit_manifest_entry "$name" "$key" "$(rel_path "$clean_file")" "$comp" "$mtype" "$offset" \
    "$orig_hex" "$mut_hex" "$orig_sha" "$corr_sha" "$orig_len" "$corr_len" \
    "$exp" "$errc" "$rationale" "active" "$clean_table"

  printf "%-30s %-20s %-12s %-9s %s\n" "$name" "$comp" "$mtype" "$offset" "$status" >>"$REPORT_TMP"
  {
    echo "  $name:"
    echo "      component        : $comp"
    echo "      clean   SHA256   : $orig_sha  ($orig_len bytes)"
    echo "      corrupt SHA256   : $corr_sha  ($corr_len bytes)"
    echo "      mutation         : $mtype @ $offset  (orig=$orig_hex mut=$mut_hex)"
    echo "      single-mutation  : $status"
  } >>"$REPORT_TMP"
done

{
  echo "----------------------------------------------------------------------------------------"
  echo "active fixtures : ${#GENERATED[@]}  (${GENERATED[*]:-none})"
  echo "planned fixtures: ${#PLANNED[@]}  (${PLANNED[*]:-none})"
  echo "================================================================"
} >>"$REPORT_TMP"

# Persist manifest + report.
mv "$MANIFEST_TMP" "$MANIFEST"
mv "$REPORT_TMP" "$REPORT"
trap - EXIT

# Committed machine-readable SHA256 record (clean -> corrupt) per fixture.
SHA_RECORD="$CORRUPT_DIR/corruption-sha256.txt"
python3 - "$MANIFEST" >"$SHA_RECORD" <<'PY'
import sys, re
txt = open(sys.argv[1]).read()
print("# fixture  component  status  original_sha256  corrupted_sha256")
for b in re.split(r"\n  - name: ", txt)[1:]:
    name = b.splitlines()[0].strip()
    def f(k):
        m = re.search(rf"^    {k}: (.+)$", b, re.M)
        return m.group(1).strip().strip('"') if m else "-"
    print(f"{name}\t{f('component')}\t{f('status')}\t{f('original_sha256') or '-'}\t{f('corrupted_sha256') or '-'}")
PY

# A README so the corrupt corpus dir is self-describing.
cat >"$CORRUPT_DIR/README.md" <<EOF
# test_comp_corrupt — Corrupted-component fixture corpus (epic #970, issue #999)

Deterministic single-component corruptions of the clean \`test_comp\` (nb/BIG)
and \`test_da\` (BTI/\`da\`) fixtures. Each subdirectory is a COMPLETE SSTable
component directory copied from a clean source with EXACTLY ONE intentional
mutation applied to ONE component.

- Generator: \`test-data/scripts/generate-corruption-corpus.sh\` (no Docker; pure offline transform)
- Manifest : \`corruption-manifest.yml\` (machine-readable; committed)
- Report   : \`verification-report.txt\` (single-mutation proof + SHA256 before/after; committed)

## Regenerate (byte-identical)

\`\`\`bash
bash test-data/scripts/generate-corruption-corpus.sh
# verify only:
bash test-data/scripts/generate-corruption-corpus.sh --verify-only
\`\`\`

The corrupted \`*.db\` binaries are **gitignored** (like all clean \`*.db\`). They
are regeneratable byte-for-byte from the committed manifest + clean sources, so
CI consumes the DESCRIBED corruptions and never mutates bytes at test time.
EOF

log "=== corruption corpus complete ==="
log "manifest: $MANIFEST"
log "report  : $REPORT"
echo
cat "$REPORT"

# Print SHA256 before/after evidence block (explicit, greppable).
echo
echo "[corrupt] SHA256 before/after evidence:"
python3 - "$MANIFEST" <<'PY'
import sys, re
txt = open(sys.argv[1]).read()
blocks = re.split(r"\n  - name: ", txt)
for b in blocks[1:]:
    name = b.splitlines()[0].strip()
    def field(k):
        m = re.search(rf"^    {k}: (.+)$", b, re.M)
        return m.group(1).strip().strip('"') if m else "?"
    print(f"  {name:30s} {field('status'):8s} {field('original_sha256')[:16]}.. -> {field('corrupted_sha256')[:16]}..")
PY

if [[ "${#PLANNED[@]}" -gt 0 ]]; then
  log "PLANNED (no clean source available in this checkout): ${PLANNED[*]}"
fi

# Fail loud if any active fixture failed its single-mutation proof.
if grep -q "FAIL" "$REPORT"; then
  fail "One or more fixtures failed the single-mutation proof (see $REPORT)."
fi
log "All active fixtures passed the single-mutation proof."
