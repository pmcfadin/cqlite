#!/usr/bin/env bash
#
# check-dataset-manifest.sh (issue #1230)
#
# Fail-closed CI sanity check: assert that the fetched dataset asset contains a
# Data.db for EVERY expected (keyspace, table) in the 39-table enforced corpus
# (33 nb + 6 test_oa), not just test_basic/simple_table. A partial extraction or
# a dropped table reds CI here instead of silently turning the dataset-dependent
# test lanes green by letting them skip on absence. The enforced corpus is
# defined in test-data/validation-matrix.md ("Enforced Tables": 39); test_da and
# test_deltas are skip-pending and intentionally NOT enforced here.
#
# Usage: check-dataset-manifest.sh [DATASETS_ROOT]
#   DATASETS_ROOT defaults to test-data/datasets (the parent of sstables/).
#
set -euo pipefail

ROOT="${1:-test-data/datasets}"
SSTABLES="${ROOT}/sstables"

# Expected user-keyspace tables (39 total: 33 nb + 6 test_oa).
#
# SINGLE SOURCE OF TRUTH (intent): this list is hand-duplicated from the corpus
# definition. Keep it in sync with test-data/validation-matrix.md ("Enforced
# Tables": 39) and the 8-table EXPECTED_TEST_BASIC_TABLES const in
# cqlite-core/tests/reader_compression_tests.rs. A future change should derive
# all of these from metadata.yml / validation-matrix.md so a table add/rename
# updates every copy at once.
EXPECTED=(
  # test_basic (8)
  "test_basic/composite_key_table"
  "test_basic/compression_test_table"
  "test_basic/counters"
  "test_basic/multi_partition_table"
  "test_basic/simple_table"
  "test_basic/static_columns_table"
  "test_basic/ttl_test_table"
  "test_basic/uncompressed_table"
  # test_collections (8)
  "test_collections/collection_clustering_table"
  "test_collections/collection_table"
  "test_collections/collections_with_udts"
  "test_collections/empty_collections_table"
  "test_collections/frozen_collections_table"
  "test_collections/large_collections_table"
  "test_collections/nested_collections_table"
  "test_collections/typed_collections_table"
  # test_timeseries (9)
  "test_timeseries/app_metrics"
  "test_timeseries/event_store"
  "test_timeseries/log_entries"
  "test_timeseries/sensor_data"
  "test_timeseries/stock_prices"
  "test_timeseries/tick_data"
  "test_timeseries/time_bucketed_counters"
  "test_timeseries/user_activity"
  "test_timeseries/user_sessions"
  # test_wide_rows (8)
  "test_wide_rows/chat_messages"
  "test_wide_rows/document_versions"
  "test_wide_rows/large_blob_table"
  "test_wide_rows/many_columns_table"
  "test_wide_rows/multi_metric_timeseries"
  "test_wide_rows/product_catalog"
  "test_wide_rows/sparse_data_table"
  "test_wide_rows/wide_partition_table"
  # test_oa (6) — the OA-format keyspace enforced by validation-matrix.md
  "test_oa/collection_table"
  "test_oa/simple_table"
  "test_oa/static_table"
  "test_oa/tombstone_table"
  "test_oa/ttl_table"
  "test_oa/udt_table"
)

# EXIT 9 IS THIS SCRIPT'S CORPUS VERDICT, and it is reserved (issue #3493 round 25).
# Callers that need to distinguish "the corpus is incomplete" from "this script did not
# get to judge" cannot use exit 1 for the former: `set -e`, a failed `grep`, an unset
# variable or any other internal error also surfaces as 1, so a BROKEN CHECKER would be
# indistinguishable from a judged verdict -- and the agent gate suppresses its Node
# dataset half on the verdict while failing closed on a tooling failure.
#
# So: 0 = complete, 9 = judged INCOMPLETE, anything else = did not judge. Ordinary
# callers (`run: bash check-dataset-manifest.sh ...`) are unaffected: 9 is still nonzero.
MANIFEST_INCOMPLETE_RC=9

if [ ! -d "$SSTABLES" ]; then
  echo "❌ dataset manifest check: sstables dir missing: $SSTABLES" >&2
  exit "$MANIFEST_INCOMPLETE_RC"
fi

# COMMITTED table dirs, mirroring parity-utils.js::isCommittedTableDir (roborev #3493
# round 18). Jest enforces parity only for table directories the SOURCE TREE git-tracks
# (#1319), so a replacement UUID on disk is discovered by neither ALL_TABLES nor
# OA_TABLES -- and this manifest would still have reported the table PRESENT, making
# "manifest OK" imply a parity run that never happens.
#
# The source tree is this SCRIPT's repo, deliberately, not the dataset root being
# checked: the two are routinely different (a fleet root such as /data/datasets sits
# outside any work tree) and the git rule is about what the CHECKOUT considers
# committed. Verified before adopting: all 39 expected tables resolve to tracked
# directories on both the fetched and the fleet corpus.
#
# Jest's graceful fallback is copied too -- an EMPTY tracked set means "git unavailable
# or not a work tree", and then every discovered dir counts. Without it this check would
# reject everything in a git-less environment, which is a louder version of the vacuous
# pass it exists to prevent.
# EXIT 9 IS ONLY RESERVED IF NOTHING ELSE CAN REACH IT (roborev #3493 round 26). The
# previous shape swallowed helper failures with `|| true` and then continued to the
# deliberate exit 9, so a broken `awk` or `sort` was reported as a judged INCOMPLETE
# corpus -- and the gate suppresses on that verdict. A reserved code that a malfunction
# can produce is not reserved.
#
# Two changes make it hold: the tools are verified UP FRONT, and every helper failure
# below propagates as 2 (did-not-judge) rather than being absorbed. The only tolerated
# absence is git-not-a-work-tree, which is a legitimate environment (Jest's own fallback)
# and is now tested for EXPLICITLY rather than inferred from an empty result -- an empty
# result is exactly what a failure also produces.
for _tool in find grep sort awk sed basename cmp git tr; do
  command -v "$_tool" >/dev/null 2>&1 || {
    echo "❌ dataset manifest check: required tool '$_tool' not found; cannot judge the corpus" >&2
    exit 2
  }
done

# `${0%/*}` rather than `dirname` for the same reason as the keyspace above: no
# subprocess, no failure mode. The `case` handles an invocation with no slash, where
# `${0%/*}` would otherwise yield $0 unchanged.
case "$0" in */*) _SCRIPT_DIR=${0%/*} ;; *) _SCRIPT_DIR=. ;; esac
_SCRIPT_REPO=$(cd "$_SCRIPT_DIR/../.." 2>/dev/null && pwd) || _SCRIPT_REPO=""
# Is _SCRIPT_REPO actually a git work tree? Established ONCE. The script is also run from a
# COPY outside any repo (the self-test's nogit fixture, and anyone who vendors it), where
# `git ls-files` exits 128 -- which is a real malfunction status everywhere else, so without
# this flag the trusted-inventory lookup would abort every such run. "Not a work tree" is a
# DECLARED absence of an inventory, not a broken tool.
_SCRIPT_REPO_IS_GIT=0
if [ -n "$_SCRIPT_REPO" ] && command -v git >/dev/null 2>&1 \
   && git -C "$_SCRIPT_REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  _SCRIPT_REPO_IS_GIT=1
fi

COMMITTED_TABLE_DIRS=""
if [ -n "$_SCRIPT_REPO" ] \
   && command -v git >/dev/null 2>&1 \
   && git -C "$_SCRIPT_REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  # `-z`, matching the node twin (`git -C ... ls-files -z` in parity-utils.js) and this
  # repo's standing rule that a path-reading git invocation is NUL-delimited (#3229's
  # structural assert says so for `git diff`, and the reason is identical here).
  #
  # WITHOUT it git QUOTES any path outside the portable charset -- `core.quotePath` turns
  # `sstables/tëst/...` into `"test-data/datasets/sstables/t\303\253st/..."` -- and the
  # `$4/$5` extraction then yields a key with a leading quote that matches no directory on
  # disk. The table would read as uncommitted, be skipped, and the corpus reported
  # incomplete: a false-MISSING that no test with an ASCII-only corpus can see.
  # A TEMP FILE, not `$(...)`. Command substitution STRIPS NUL BYTES -- bash even warns
  # "ignored null byte in input" -- so capturing `ls-files -z` that way collapses the whole
  # listing into one line, the loop below extracts NOTHING, and the empty result silently
  # takes the "nothing tracked -> everything counts" fallback. That is FAIL-OPEN: every
  # directory becomes committed and the #1319 WIP-dir guard is gone. It is invisible from
  # the script's own output, which still prints a green 39/39, because the fallback is a
  # legitimate state -- exactly the vacuous-pass shape this whole change is about. Caught
  # only by counting the derived entries.
  _ls_tmp=$(mktemp) || {
    echo "❌ dataset manifest check: mktemp failed; cannot derive the committed table set" >&2
    exit 2
  }
  if ! git -C "$_SCRIPT_REPO" ls-files -z test-data/datasets/sstables >"$_ls_tmp" 2>/dev/null; then
    rm -f "$_ls_tmp"
    echo "❌ dataset manifest check: 'git ls-files' failed; cannot derive the committed table set" >&2
    exit 2
  fi
  _ctd=""
  while IFS= read -r -d '' _p; do
    # test-data/datasets/sstables/<keyspace>/<table-dir>/<file>
    _rest=${_p#test-data/datasets/sstables/}
    [ "$_rest" = "$_p" ] && continue
    _ks=${_rest%%/*}; _rest=${_rest#*/}
    case "$_rest" in */*) : ;; *) continue ;; esac      # need a file BELOW the table dir
    _tbl=${_rest%%/*}
    [ -n "$_ks" ] && [ -n "$_tbl" ] && _ctd="$_ctd$_ks/$_tbl"$'\n'
  done <"$_ls_tmp"
  rm -f "$_ls_tmp"
  # GUARDED, and every nonzero normalised to 2 (roborev #3493 round 59). Under `set -e` an
  # unguarded assignment propagates the pipeline's RAW status as the script's exit -- and if
  # `sort` or `sed` ever exited 9, that is this script's RESERVED corpus verdict, so a
  # TOOLING FAILURE would be read as a judged "incomplete corpus" and suppressed by the
  # #2078 opt-out. Only a code this script emits DELIBERATELY may carry a verdict; that is
  # the whole basis of the reserved-exit discipline (rounds 25/27), and an unguarded
  # pipeline quietly opts out of it.
  if ! COMMITTED_TABLE_DIRS=$(printf '%s' "$_ctd" | sort -u | sed '/^$/d'); then
    echo "❌ dataset manifest check: could not derive the committed table set (sort/sed failed); cannot judge the corpus" >&2
    exit 2
  fi
fi

# _re_match <ERE> <string> -- rc 0 WHOLE-STRING match, rc 1 NO MATCH, exit 2 on MALFUNCTION.
#
# grep returns 1 for "no match" and >1 for an operational failure, and collapsing the two
# is how a broken checker reaches this script's deliberate exit 9 (roborev #3493 round
# 27). `|| continue` and `&& return 0` both read >1 as an ordinary non-match, the loop
# falls through, and the run ends in a judged INCOMPLETE verdict that the gate's opt-out
# then suppresses. Every regex predicate goes through here so that distinction is made
# once, in one place, rather than at each of the sites that needs it.
_re_match() {
  # BASH `[[ =~ ]]`, NOT grep (roborev #3493 round 59). grep matches LINE BY LINE, so an
  # anchored `^...$` accepts a value whose SOME line matches -- and a filename may contain a
  # newline. Measured: for the two-line name `oa-x\noa-1-big-Data.db`,
  # `grep -Eq '^oa-[0-9]+-big-Data\.db$'` says MATCH while the JS consumer
  # `/^oa-\d+-big-Data\.db$/.test()` says NO (JS `$` without `m` anchors to end of INPUT).
  # That is a false-PRESENT: the manifest reports the corpus complete, Jest's
  # `oaBinariesPresent()` does not see the file, and the suite fails with the opt-out unable
  # to suppress it. `[[ =~ ]]` matches the WHOLE STRING, exactly like the JS side.
  #
  # It also removes the SIGPIPE and grep-malfunction hazards this function existed to
  # manage -- there is no external process left -- while KEEPING the three-valued contract:
  # bash returns 2 for an INVALID REGEX, which is a genuine malfunction and must not be
  # collapsed onto "no match" (that is how a broken checker reached the reserved exit 9 in
  # round 27).
  #
  # `local LC_ALL=C` because bash evaluates the ERE through the C library under the CURRENT
  # locale, so ranges like `[a-z]` are collation-dependent -- the same locale trap as the
  # whitespace and control-character predicates (rounds 45/46/48). Dynamic scoping restores
  # it on return.
  local LC_ALL=C
  local _re=$1 _rc=0
  [[ $2 =~ $_re ]] || _rc=$?
  case "$_rc" in
    0) return 0 ;;
    1) return 1 ;;
    *) echo "❌ dataset manifest check: invalid regex (status $_rc) matching '$1'; cannot judge the corpus" >&2
       exit 2 ;;
  esac
}

# _reader_accepts_descriptor <basename> -- rc 0 iff CQLite's readers would open a file
# with this name. Mirrors the VERSION GATES rather than a name shape (roborev #3493
# round 36):
#   * BIG (`<ver>-<gen>-big-Data.db`) -- BigVersionGates::from_version accepts any TWO
#     LOWERCASE LETTERS at or above the `na` floor; pre-`na` (ma..me, Cassandra 3.x) is
#     out of scope and rejected there, so it is rejected here.
#   * BTI (`<ver>-<gen>-bti-Data.db`) -- BtiVersionGates::from_version accepts ONLY `da`.
# The previous `(big|bti)` alternation ignored the pairing, so `nb-1-bti` -- a version the
# BTI gate rejects outright -- counted as a readable fixture.
#
# The BIG set is an EXACT ALLOWLIST `{na, nb, oa}`, not a floor -- #1249 sets the floor,
# #1297 adds the ceiling. `BigVersionGates::from_version` on main:
#
#     if v < "na" { return Err(Error::UnsupportedVersion { .. }); }   // #1249 floor
#     ...
#     // #1297: the supported set is an EXACT allowlist, not just a floor.
#     if !matches!(v, "na" | "nb" | "oa") {
#         return Err(Error::UnsupportedVersion { .. });               // #1297 ceiling
#     }
#
# A second consumer agrees independently: `FormatDetector::is_supported()` is
# `V4x => matches!(v, "na"|"nb")`, `V5x => matches!(v, "oa"|"da")`, and
# `supported_versions()` is derived from it. Two consumers, same set.
#
# So an above-floor-but-unlisted version (`nc`, a typo like `nz`, a future `pa`) has NO
# validated read path and the reader ERRORS on it. Accepting it here would make this
# check LOOSER than the reader -- the false-PRESENT class this whole change exists to
# remove -- so `zz-9-big` is rejected, not accepted.
#
# A genuine future format is added deliberately once validated; one line in two places
# on that day is the intended cost of keeping no-heuristics true.
#
# HISTORY, so this is not re-argued a fourth time: rounds 36-38 of review all flagged
# this, and I twice asserted there was no allowlist. I was wrong. The allowlist is at
# big.rs:133 and was in the tree the whole time -- both of my reads were WINDOWED
# (`+22p`, then a comment-stripped `20,60p`) and neither window reached it. Absence
# concluded from a partial view of a function is not evidence of absence.
#
# No generation bound: the generation is u64 (parser/header.rs:377) via an
# Option-returning helper, so there is no range to mirror.
_reader_accepts_descriptor() {
  local _b=$1 _ver _fmt
  case "$_b" in
    *-big-Data.db) _fmt=big ;;
    *-bti-Data.db) _fmt=bti ;;
    *) return 1 ;;
  esac
  # The SSTable ID may be SEQUENTIAL or a UUID (roborev #3493 round 39).
  # SSTableInfo::from_path accepts both -- `nb-1-big-Data.db` and
  # `nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db` -- and its own doc notes that real
  # Cassandra 5.0 clusters write UUID ids BY DEFAULT (`uuid_sstable_identifiers_enabled:
  # true`). A `[0-9]+`-only pattern therefore rejected the id form the reader will most
  # often see: over-strict, and it would have red-lined an ordinary Cassandra 5 corpus.
  # `generation_numeric()` returning Option is the same fact from the other side -- a
  # non-numeric id is expected, not an error.
  #
  # HYPHENS are allowed in the id because from_path builds it as
  # `parts[1..format_idx].join("-")` -- a multi-segment (hyphenated) UUID is re-joined, so
  # `nb-6aa08200-a251-11f0-a3fe-f1a551383fb9-big-Data.db` is a valid descriptor
  # (roborev #3493 round 40). The class cannot swallow the format segment: `big`/`bti`
  # both contain non-hex letters, so the anchored match still finds the right boundary.
  #
  # The OA branch stays NUMERIC-ONLY on purpose. Its consumers are Jest's
  # `oaBinariesPresent()` (`/^oa-\d+-big-Data\.db$/`) and `findOaJsonlFile()`
  # (`/^oa-\d+-big-Data\.db\.jsonl$/`) -- both digits-only. Accepting a UUID OA id here
  # would make this check LOOSER than the consumer: the fixture would pass the manifest
  # and then be skipped by Jest, which is the false-PRESENT class. Widening Jest's own
  # discovery is a separate change; the corpus has only numeric OA ids today.
  # The ID is ARBITRARY, not hex (roborev #3493 round 56). `parse_filename` validates
  # exactly two things -- a 2-lowercase-letter version and a `big`/`bti` format segment --
  # and takes `parts[1..format_idx]` as the id with NO charset restriction, so
  # `nb-foo-big-Data.db` parses. A `[0-9a-f-]+` class was therefore STRICTER THAN THE
  # READER, and this check being stricter is the false-MISSING direction: such a file is
  # not counted as a generation, so a corpus holding only that one reads as absent and the
  # Node suite is suppressed for a corpus the reader can open.
  #
  # `.*` rather than `.+`: the parser accepts an empty id segment (`nb--big-Data.db` splits
  # to 4 parts and passes both of its checks), and mirroring it exactly is the point --
  # "no writer emits that shape" is the same reasoning that made the hex class wrong.
  #
  # GREEDY `.*` mirrors the parser's RIGHT-TO-LEFT scan for the format segment, so a
  # pathological `nb-x-big-y-big-Data.db` resolves its id and format identically on both
  # sides. Version and format remain gated below -- widening the ID does not widen those.
  _re_match '^[a-z][a-z]-.*-(big|bti)-Data\.db$' "$_b" || return 1
  _ver=${_b%%-*}
  case "$_fmt" in
    bti) [ "$_ver" = da ] || return 1 ;;                       # BtiVersionGates: only `da`
    big) case "$_ver" in na|nb|oa) ;; *) return 1 ;; esac ;;   # BigVersionGates: exact allowlist
  esac
  return 0
}

# _usable_file <path> -- rc 0 iff it is a NONEMPTY REGULAR FILE (or a symlink to one).
#
# `[ -s x ]` alone is NOT that test: it is true for a NONEMPTY DIRECTORY (roborev #3493
# round 42). So a directory named `nb-1-big-Data.db.jsonl`, or a directory-valued
# `nb-1-big-Data.db` beside a real binary, satisfied the size check and the manifest
# reported the corpus complete -- after which Jest fails reading it and the opt-out cannot
# suppress, because nothing classified the corpus as incomplete.
#
# `-f` supplies the type half and FOLLOWS symlinks, so a symlink to a nonempty regular
# file still counts -- the same rule as _canonical_fixture_present and setup.js's
# hasDataDbFile. Every binary and golden goes through here so no site can drift again:
# round 35 gave binaries the size rule and missed goldens, round 41 gave goldens the size
# rule and missed the TYPE, and both were one-site-at-a-time edits.
# READABLE too (roborev #3493 round 43). `-f`/`-s` say a file exists with content; they
# say nothing about whether this process can OPEN it. An unreadable Data.db or golden was
# therefore classified as present, the decision returned RUN, and the consumer failed with
# a permission error that the opt-out could not pre-empt -- the same fail-open shape as
# every other state this predicate exists to catch. The Rust resolver already requires a
# READABLE directory (fixture_roots.rs), so this aligns with it rather than inventing a
# rule.
_usable_file() { [ -f "$1" ] && [ -s "$1" ] && [ -r "$1" ]; }

# _toc_companions_usable <data-db-path> -- rc 0 iff every component the generation's
# OWN `TOC.txt` lists is present beside it as a READABLE REGULAR FILE.
#
# Why (roborev #3493 round 47): a nonempty Data.db plus a golden was the whole
# completeness test, so a PARTIAL EXTRACTION that dropped a companion -- the
# `CompressionInfo.db` that 126 of this corpus's 172 table directories have -- still
# reported SATISFIED. The decision was then RUN, and Jest failed on an unreadable
# compressed SSTable instead of the corpus being classified incomplete and suppressed,
# which `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` could not rescue.
#
# TOC.txt is CASSANDRA'S OWN manifest of the generation's components, so the required
# set is DERIVED from the fixture rather than curated here: a compressed generation
# lists `CompressionInfo.db`, an uncompressed one does not, and a BTI generation lists
# `Partitions.db`/`Rows.db` instead of `Index.db`. Nothing to update when a new
# component appears.
#
# Its ABSENCE disqualifies rather than skipping the check. Measured: all 144 generations
# across both corpus roots have one, and a permissive branch keyed on a missing signal is
# the vacuous pass this whole component exists to prevent -- a fetch truncated badly
# enough to drop TOC.txt is exactly the state to report.
#
# PRESENCE + TYPE + READABILITY, and NONEMPTY except for a derived allowlist.
#
# Round 53 allowed every companion to be zero-length, on the measurement that 3 of the 1152
# TOC-listed components on the real corpus legitimately are. That was the right measurement
# and the wrong rule (roborev #3493 round 58): it also admitted the two shapes that BREAK
# the reader. Measured against the real node binding, zeroing ONE component of an otherwise
# intact generation:
#
#     CompressionInfo.db  -> SELECT returns 0 ROWS      <-- silently wrong
#     Statistics.db       -> SELECT returns 0 ROWS      <-- silently wrong
#     Filter.db           -> 100 rows (tolerated)
#     Index.db            -> 100 rows (tolerated)
#     Summary.db          -> 100 rows (tolerated)
#     Digest.crc32        -> 100 rows (tolerated)
#
# It does not throw -- it returns an EMPTY RESULT SET, which is worse: "0 rows when the
# fixture is present" is the exact failure this repo's testing doctrine says must never
# pass, and a suite that only counts rows would report success over a broken corpus.
#
# So nonempty is REQUIRED, with `Rows.db` allowlisted. That allowlist is DERIVED, not
# invented: it is every component observed zero-length across all 144 generations in both
# corpus roots -- 3 instances, all `da-2-bti-Rows.db`, where an empty row index is a
# meaningful BTI state rather than damage.
#
# IT IS NOT LOAD-BEARING TODAY, and saying otherwise would overstate it: all 3 live in
# `test_da/` tables, and the expected set holds none of them, so a blanket nonempty rule
# would also pass the real corpus 39/39. It is here because the SHAPE is real -- Cassandra
# writes an empty BTI row index -- and the expected set plausibly grows to cover BTI. The
# synthetic case is what discriminates, not the corpus.
#
# If a future corpus legitimately ships another empty component this reds and the allowlist
# gains a line; that is the correct direction, because the alternative admits a
# silently-wrong read.
#
# The Data.db's own nonzero requirement is enforced separately by its caller.
_TOC_MAY_BE_EMPTY='Rows.db'

# _TOC_SIDECAR_GLOBS -- files sharing a generation prefix that are NOT Cassandra components
# and are therefore legitimately absent from the TOC. Measured across all 144 generations in
# both corpus roots: exactly two shapes, both human-readable dumps this TEST corpus ships
# (142 `Statistics.db.txt`, 6 `CompressionInfo.db.txt`) plus the `.jsonl` goldens.
_TOC_SIDECAR_GLOBS='*.jsonl *.db.txt'
_toc_companions_usable() {
  # Which of this function's several rejections fired, so the caller can NAME the cause
  # instead of reporting them all as "the TOC lists something missing" (the round-40 rule).
  _TOC_FAIL_REASON=listed-component
  _t_prefix=${1%Data.db}
  _t_toc="${_t_prefix}TOC.txt"
  _usable_file "$_t_toc" || return 1
  while IFS= read -r _t_c || [ -n "$_t_c" ]; do
    _t_c=${_t_c%$'\r'}                       # tolerate CRLF
    [ -n "$_t_c" ] || continue               # blank lines are not components
    # A component NAME, never a path: a TOC entry that escapes its own directory is
    # malformed, and resolving it would validate a file in a different generation.
    case "$_t_c" in */*|.|..) return 1 ;; esac
    _t_p="${_t_prefix}${_t_c}"
    [ -f "$_t_p" ] && [ -r "$_t_p" ] || return 1
    case " $_TOC_MAY_BE_EMPTY " in
      *" $_t_c "*) : ;;                      # allowlisted: empty is a legitimate state
      *) [ -s "$_t_p" ] || return 1 ;;
    esac
  done < "$_t_toc"
  # BIDIRECTIONAL (roborev #3493, post-rebase round). The loop above proves every LISTED
  # component exists; on its own that trusts the TOC as a COMPLETE inventory, so a
  # TRUNCATED-but-nonempty TOC listing only `Data.db` shrinks the required set to nothing
  # and the partial extraction this check exists to catch walks through.
  #
  # So the other direction too: every file sharing this generation's prefix must BE LISTED.
  # A truncation that drops `CompressionInfo.db` from the TOC is then caught by the file
  # still being on disk -- which is the shape a half-written TOC actually leaves.
  #
  # A mandatory-component floor was tried first and is NOT this: it cannot catch a truncated
  # TOC whose components all happen to be present, and it red-lined 40 synthetic fixtures for
  # a reason unrelated to what they test.
  for _t_f in "${_t_prefix}"*; do
    [ -e "$_t_f" ] || continue
    _t_c=${_t_f#"$_t_prefix"}
    _t_skip=0
    for _t_g in $_TOC_SIDECAR_GLOBS; do
      case "$_t_c" in $_t_g) _t_skip=1; break ;; esac
    done
    [ "$_t_skip" -eq 1 ] && continue
    # STATUS DISCIPLINE, as everywhere else in this script (roborev, post-rebase round 2).
    # `grep ... || return 1` collapses an OPERATIONAL failure (>1) onto "not listed", and
    # because this function is called on the left of `||` that non-match walks out to the
    # reserved exit 9 -- a judged corpus verdict the #2078 opt-out suppresses. A broken
    # grep must never be readable as a judged corpus.
    _t_g=0
    grep -qxF "$_t_c" "$_t_toc" || _t_g=$?
    case "$_t_g" in
      0) : ;;
      1) return 1 ;;
      *) echo "❌ dataset manifest check: grep failed (status $_t_g) reconciling $_t_toc; cannot judge the corpus" >&2
         exit 2 ;;
    esac
  done
  # THE TRUSTED INVENTORY (roborev, post-rebase round 2). Both directions above are derived
  # from the CORPUS, so a COHERENTLY truncated TOC -- one shortened in step with the files
  # it stopped listing -- satisfies each of them and greens an incomplete generation.
  #
  # `*-TOC.txt` is GIT-TRACKED (164 committed), so it is not subject to the truncated fetch
  # that damages the gitignored binaries: it is an inventory from OUTSIDE the corpus being
  # judged, which is exactly what the derived checks cannot be. Measured: all 144 generations
  # in the machine-local corpus have a committed twin and all 144 match byte-for-byte, so
  # requiring the match costs nothing today and is fail-closed.
  #
  # NO TWIN => the derived checks alone, and this is a DECLARED LIMIT rather than a hidden
  # one. Making an absent twin fatal was tried and is wrong: a corpus legitimately holds
  # tables this checkout does not commit (a scratch root, an out-of-tree corpus, a
  # newly-generated table), and there is no trusted inventory for those BY DEFINITION, so
  # rejecting them reports "incomplete" about something that is merely unverifiable. It also
  # red-lined 41 synthetic fixtures whose tables cannot have a twin.
  #
  # THE RESIDUAL, stated because it is real: for a generation with no committed twin, a
  # COHERENT truncation is undetectable here. It does not affect the corpus the gate actually
  # runs against — all 144 generations there have a twin — but a corpus assembled elsewhere
  # gets the weaker guarantee, and nothing in the output would otherwise say so.
  _TOC_FAIL_REASON=untrusted-toc
  # Plain call, NOT `$(...)`: see the function header -- a subshell would swallow its exit 2.
  _committed_toc_relpath "$_t_toc"
  if [ -n "$_C_TOC_REL" ]; then
    _toc_matches_head "$_t_toc" "$_C_TOC_REL" || return 1
  fi
  _TOC_FAIL_REASON=
  return 0
}

# _committed_toc_relpath <corpus-toc-path> -- set _C_TOC_REL to the REPO-RELATIVE path of
# this TOC's git-tracked twin, or to the empty string when there is none.
#
# SETS A GLOBAL RATHER THAN ECHOING, deliberately. Called as `$(...)` it would run in a
# SUBSHELL, so the `exit 2` below would kill only that subshell; the caller's `|| return 1`
# would then turn a MALFUNCTION into "TOC mismatch" and walk it out to the reserved exit 9 --
# the very collapse this discipline exists to prevent, reintroduced by the call syntax.
#
# git distinguishes the two cases and so must this: `ls-files --error-unmatch` exits 1 for
# UNTRACKED (a legitimate "no twin") and 128 for a broken invocation / not-a-repo. Measured.
# Collapsing 128 onto "no twin" would silently disable the trusted-inventory check.
_committed_toc_relpath() {
  _C_TOC_REL=""
  _c_tail=${1#*/sstables/}
  [ "$_c_tail" = "$1" ] && return 0
  # Not a work tree => no inventory exists to compare against. Declared, not a malfunction.
  [ "$_SCRIPT_REPO_IS_GIT" = 1 ] || return 0
  _c_rel="test-data/datasets/sstables/$_c_tail"
  _c_rc=0
  git -C "$_SCRIPT_REPO" ls-files --error-unmatch "$_c_rel" >/dev/null 2>&1 || _c_rc=$?
  case "$_c_rc" in
    0) _C_TOC_REL="$_c_rel" ;;
    1) : ;;                                  # untracked: no twin, fall back to the derived checks
    *) echo "❌ dataset manifest check: 'git ls-files' failed (status $_c_rc) resolving the committed twin of $1; cannot judge the corpus" >&2
       exit 2 ;;
  esac
  return 0
}

# _toc_matches_head <corpus-toc-path> <repo-relative-path> -- rc 0 iff the corpus TOC matches
# the content COMMITTED AT HEAD.
#
# READ FROM `git show HEAD:<path>`, NOT FROM THE WORKING TREE (roborev, post-rebase round 3).
# The first version compared against the working-tree file, and under the DEFAULT dataset
# root -- the checkout's own `test-data/datasets` -- those are THE SAME FILE. `cmp` compared
# a file to itself, always succeeded, and the trusted-inventory check was VACUOUS in exactly
# the configuration CI uses. Verified: both paths resolve to the identical absolute path.
#
# HEAD rather than the index (`git show :<path>`): a staged-but-uncommitted truncation would
# otherwise be its own authority, and the inventory has to come from somewhere the corpus
# under judgement cannot reach.
#
# STATUS DISCIPLINE, as everywhere in this script: `cmp` exits 0 same, 1 different, >1
# TROUBLE (unreadable, or absent from PATH). Collapsing >1 onto "different" would walk a
# tooling failure out to the reserved exit 9, which the #2078 opt-out suppresses as a judged
# corpus. `cmp` is in the up-front tool check for the same reason.
_toc_matches_head() {
  # mktemp, not a `$$`-derived name: a predictable path in a shared /tmp is both a symlink
  # target and, if a previous `rm -f` ever failed, a stale file this would compare against.
  _tm_tmp=$(mktemp "${TMPDIR:-/tmp}/cqlite-toc-head.XXXXXX") || {
    echo "❌ dataset manifest check: mktemp failed; cannot materialise the committed twin" >&2
    exit 2
  }
  # A CONFIRMED absence from HEAD is not an operational failure, and collapsing the two
  # (roborev, post-rebase round 4) would let git corruption or a permission error read as
  # "no inventory" and silently disable this check. `cat-file -e` answers EXISTENCE on its
  # own, so a `show` that fails afterwards is a genuine malfunction.
  if ! git -C "$_SCRIPT_REPO" cat-file -e "HEAD:$2" 2>/dev/null; then
    rm -f "$_tm_tmp"
    # Tracked but absent at HEAD (added, not yet committed): the inventory does not exist
    # yet, so fall back to the derived checks rather than invent one.
    return 0
  fi
  if ! git -C "$_SCRIPT_REPO" show "HEAD:$2" >"$_tm_tmp" 2>/dev/null; then
    rm -f "$_tm_tmp"
    echo "❌ dataset manifest check: 'git show HEAD:$2' failed although the object EXISTS; cannot judge the corpus" >&2
    exit 2
  fi
  # COMPARE THE INVENTORY, NOT THE BYTES (roborev, post-rebase round 4). A byte-for-byte
  # `cmp` rejects a CRLF TOC -- which the listed-component loop above explicitly TOLERATES
  # and which the reader trims. Requiring byte equality here while accepting CRLF twenty
  # lines up is an internal contradiction, and it would mark a valid Windows-produced corpus
  # incomplete. The property is that the two list the SAME COMPONENTS, so that is what is
  # compared: CR stripped, blanks dropped, `sort`ed -- a TOC is an inventory, not a sequence.
  _tm_a="$_tm_tmp.a"; _tm_b="$_tm_tmp.b"
  if ! tr -d '\r' <"$1" | sed '/^$/d' | sort >"$_tm_a" \
     || ! tr -d '\r' <"$_tm_tmp" | sed '/^$/d' | sort >"$_tm_b"; then
    rm -f "$_tm_tmp" "$_tm_a" "$_tm_b"
    echo "❌ dataset manifest check: could not normalise $1 or its committed twin; cannot judge the corpus" >&2
    exit 2
  fi
  _tm_rc=0
  cmp -s "$_tm_a" "$_tm_b" || _tm_rc=$?
  rm -f "$_tm_tmp" "$_tm_a" "$_tm_b"
  case "$_tm_rc" in
    0) return 0 ;;
    1) return 1 ;;
    *) echo "❌ dataset manifest check: cmp failed (status $_tm_rc) comparing $1 with its committed twin; cannot judge the corpus" >&2
       exit 2 ;;
  esac
}

# _dir_has_oa_golden <table-dir> -- rc 0 when the directory holds ANY
# `oa-<n>-big-Data.db.jsonl`, which is exactly what Jest's findOaJsonlFile scans for. It
# does NOT pair the golden's generation with any binary's, so neither does this.
# EVERY matching golden must be usable, not merely one of them (roborev #3493 round 44).
# `findOaJsonlFile()` returns the FIRST readdir entry whose NAME matches, checking only
# `existsSync` -- no type, size or readability test. So a broken `oa-1-…jsonl` beside a
# valid `oa-2-…jsonl` would clear an "any usable" check here and then be the one Jest
# selects. The consumer picks blind, so the corpus is only complete if every candidate it
# could pick is usable.
_dir_has_oa_golden() {
  _found=0
  for _g in "$1"/oa-*-big-Data.db.jsonl; do
    [ -e "$_g" ] || [ -L "$_g" ] || continue
    _re_match '^oa-[0-9]+-big-Data\.db\.jsonl$' "${_g##*/}" || continue
    # A name Jest would select but cannot use makes the whole table unusable.
    _usable_file "$_g" || return 1
    _found=1
  done
  [ "$_found" -eq 1 ]
}

# _is_committed_table_dir <keyspace> <dirname> -- rc 0 when it counts.
_is_committed_table_dir() {
  [ -z "$COMMITTED_TABLE_DIRS" ] && return 0        # Jest's fallback: nothing tracked -> all count
  # Same 1-vs->1 rule as _re_match, and it is needed HERE too (roborev #3493 round 28):
  # this was the one grep site round 27 did not convert, and a nonzero-means-uncommitted
  # read lets an operational failure be swallowed by the caller's `|| continue`, ending in
  # the reserved exit 9 -- a judged verdict the opt-out suppresses. A fixed-string match
  # is a different grep invocation (`-Fxq`) from the regex ones, so converting the regex
  # sites alone left the hole open.
  # Here-string for the same reason as _re_match: under pipefail an early match through a
  # pipeline returns 141 and would be read as a malfunction. This subject is the whole
  # committed-table list, so it is the one most likely to grow past the pipe buffer.
  local _rc=0
  grep -Fxq "$1/$2" <<<"$COMMITTED_TABLE_DIRS" || _rc=$?
  case "$_rc" in
    0) return 0 ;;
    1) return 1 ;;
    *) echo "❌ dataset manifest check: grep failed (status $_rc) matching the committed table set; cannot judge the corpus" >&2
       exit 2 ;;
  esac
}

missing=0
found=0
for entry in "${EXPECTED[@]}"; do
  table="${entry#*/}"
  # Match <table>-<uuid>/<prefix>-Data.db; -path keeps us pipefail-safe.
  #
  # `-H` and the `test -f` filter make this the SAME fixture predicate as the agent
  # gate's _canonical_fixture_present and the Node suite's setup.js::hasDataDbFile
  # (issue #3493). Without them the three disagreed, and the disagreement was
  # reachable in both directions: a keyspace directory that is itself a SYMLINK is
  # followed by fs.readdirSync() and by `find -H`, but not by bare `find`, so a
  # supported corpus passed the gate's preflight and then failed this check; and a
  # DIRECTORY / FIFO / DANGLING SYMLINK named `*-Data.db` satisfied a name-only match
  # while being unopenable, so a malformed corpus could report complete.
  # No pipe (a `| head -1` would let SIGPIPE decide the result under pipefail) and no
  # `-quit`: that primary is GNU/newer-BSD only, and node-ci.yml runs this script on
  # macos-14 and windows as well as ubuntu, where an unsupported primary would error out,
  # have its stderr discarded, and report usable fixtures as MISSING.
  # The glob is COMPONENT-ANCHORED: `*/<table>-*/*-Data.db` requires an immediate
  # directory component beginning exactly with `<table>-`. The old `*<table>-*-Data.db`
  # let the leading `*` swallow a prefix, so a table whose name is a SUFFIX of a sibling
  # matched that sibling's directory and a MISSING table reported as PRESENT
  # (roborev #3493 round 15).
  #
  # LATENT, not live, and the distinction was checked rather than assumed: the search is
  # rooted at the KEYSPACE directory, so only a same-keyspace pair can collide, and the
  # current manifest has none -- `counters` (test_basic) and `time_bucketed_counters`
  # (test_timeseries) look like a collision but are searched under different roots. The
  # anchoring is kept anyway because it costs nothing, the manifest gains tables
  # routinely, and the failure mode is a silent false PRESENT: the check would report a
  # complete corpus while an expected table was absent.
  # MIRRORS JEST'S DISCOVERY EXACTLY, rather than approximating it with globs (roborev
  # #3493 rounds 16-17). Every loosening below was a real false PRESENT -- the manifest
  # reporting a complete corpus while Jest silently omitted the table:
  #   * `-path` wildcards match `/`, so a glob alone accepted the table directory nested
  #     ARBITRARILY DEEP (`other/counters-uuid/x-Data.db`); Jest reads DIRECT children only.
  #   * `${table}-*` accepted a malformed suffix (`collection_table-abc`); Jest's
  #     TABLE_DIR_RE is `^(.+)-[0-9a-f]{32}$`.
  #   * `oa-*-big-Data.db` accepted `oa-invalid-big-Data.db`; oaBinariesPresent() is
  #     `^oa-\d+-big-Data\.db$`.
  # A predicate that is LOOSER than the consumer's is not a check, it is a second opinion
  # nobody asked for -- so both regexes are copied from the consumer, and named here so a
  # future edit to either side is visibly a two-place change.
  # PARAMETER EXPANSION, not a `dirname` subprocess (roborev #3493 round 32). A subprocess
  # can fail, and this script reserves exit 9 for its corpus verdict -- so a `dirname`
  # that exited 9 would propagate as a FALSE judged-incomplete under `set -e`, and in the
  # `$( )` argument position at the committed-dir call its status was swallowed entirely.
  # `${entry%%/*}` cannot fail, needs no tool, and the entries are `<keyspace>/<table>` by
  # construction.
  keyspace=${entry%%/*}
  ks_dir="$SSTABLES/$keyspace"
  data_db=""
  first_ok=""     # the first candidate that yielded a usable fixture (success path only)
  cand_bad=0      # at least one enumerable candidate directory yields nothing usable
  oa_bad=0        # a correctly-shaped dir held Data.db files, but none OA-named
  name_bad=0      # Data.db files existed, but none named like a descriptor the reader opens
  empty_bad=0     # a correctly-named binary existed but was ZERO-LENGTH (truncated fetch)
  toc_why=""      # which TOC rejection fired: listed-component | untrusted-toc
  collide_bad=0   # a non-generation *-Data.db shares a real generation's prefix
  type_bad=0      # a correctly-named Data.db exists but is NOT a regular file
  unread_bad=0    # a correctly-named, nonempty Data.db is not readable
  toc_bad=0       # a complete-looking generation whose own TOC.txt lists an absent component
  golden_bad=0    # a valid nonempty binary existed, but without the golden Jest reads
  gen_bad=0       # a reader-supported generation in THIS candidate is unusable
  for cand in "$ks_dir/${table}"-*; do
    # `-d` FOLLOWS symlinks; Jest reads Dirent.isDirectory() from a withFileTypes walk,
    # which is FALSE for a symlink. A symlinked table dir would satisfy this check and
    # then vanish from Jest's discovered cases (roborev #3493 round 18).
    [ -L "$cand" ] && continue
    [ -d "$cand" ] || continue
    gen_bad=0     # per CANDIDATE, not per table
    base=${cand##*/}
    suffix=${base#"${table}-"}
    # Jest: TABLE_DIR_RE = /^(.+)-[0-9a-f]{32}$/
    _re_match '^[0-9a-f]{32}$' "$suffix" || continue
    # Jest: isCommittedTableDir(keyspace, dirName)
    _is_committed_table_dir "$keyspace" "$base" || continue

    # THE GOLDEN IS VALIDATED PER CANDIDATE, and the loop breaks only on a COMPLETE
    # binary+golden pair (roborev #3493 round 23). Breaking on the first binary and
    # checking its golden afterwards made a golden-less `oa-1-big-Data.db` MASK a
    # complete `oa-2` in the same directory -- reporting the table missing when Jest,
    # which scans every generation, would have found it. That direction matters as much
    # as the permissive one: an over-strict corpus check red-lines a usable corpus, and a
    # gate that reds on correct input is the gate agents learn to waive.
    #
    # The two families differ in WHAT the golden is, which is why this is not one rule:
    #   * OA     -- findOaJsonlFile scans for `oa-<n>-big-Data.db.jsonl`, any generation,
    #               so the golden is the matched binary's own sibling.
    #   * non-OA -- findJsonlFile HARD-CODES `nb-1-big-Data.db.jsonl` for the directory,
    #               regardless of which generation's Data.db is present. Measured: this
    #               corpus really holds nb-2/nb-3/nb-45/da-2-bti generations, so a
    #               per-binary sibling check was wrong about real data.
    # TWO PASSES, because two DIFFERENT questions were tangled into one loop and each
    # round of this issue had to fix them separately (roborev #3493 round 54):
    #
    #   PASS 1 -- READER COMPLETENESS, branch-independent. `Database.open` reads the table
    #             through its DIRECTORY and loads EVERY generation in it, so every
    #             reader-supported generation must be usable, whatever the table's own
    #             requirement is. Round 53 applied this to the general branch only, leaving
    #             the pinned-binary branch (which `break`s on a valid `nb-1`) and the OA
    #             branch (which never set `gen_bad`) able to mask a damaged sibling.
    #
    #   PASS 2 -- THE TABLE'S OWN REQUIREMENT, branch-specific. Which generation satisfies
    #             the CONSUMER: any recognised one, the pinned `nb-1` for the one table that
    #             names it, or a NUMERIC `oa-<n>` for Jest's OA guard.
    #
    # Separating them is the fix, not another per-branch patch: the previous shape put a
    # completeness rule inside each requirement branch, so a new branch silently arrived
    # without one. Now a branch can only choose a WINNER; it cannot opt out of validation.

    # ---- PASS 1: every reader-supported generation must be usable + TOC-complete --------
    for f in "$cand"/*-Data.db; do
      # `-e` is FALSE for a DANGLING SYMLINK, so `-e` alone skipped one silently -- the
      # very shape `type_bad` exists to report. `-L` catches it; together they mean
      # "a directory entry is here", and only an unmatched glob falls through.
      [ -e "$f" ] || [ -L "$f" ] || continue
      fbase=${f##*/}
      # Not a descriptor the reader opens => not a generation at all, and must not
      # disqualify the table (the round-24 over-rejection, one level down). `junk-Data.db`
      # and a non-numeric `oa-<uuid>-big-Data.db` both land here.
      if ! _reader_accepts_descriptor "$fbase"; then
        name_bad=1
        # PREFIX COLLISION (roborev #3493, post-rebase round). Production discovery is a
        # bare `filename.ends_with("-Data.db")` and `SSTableComponent::from_filename` maps
        # ANY such name to the Data component -- so a file sharing a REAL generation's
        # prefix is read as that generation's Data component and corrupts it.
        #
        # Measured, garbage bytes under each name beside a healthy nb-1 generation:
        #   nb-1-big-Foo-Data.db -> the query THROWS      <-- shares nb-1-big-
        #   junk-Data.db         -> 100 rows (tolerated)
        #   nb-9-big-Data.db     -> 100 rows (tolerated)  <-- valid descriptor, other gen
        #   xx-1-big-Data.db     -> 100 rows (tolerated)
        #   nb-foo-big-Data.db   -> 100 rows (tolerated)
        #
        # So the hazard is NARROWER than "any unparseable name": a non-generation file is
        # discovered, fails to open and is SKIPPED (best-effort load) UNLESS it collides
        # with a real generation's prefix. That is the only fatal shape measured, and it is
        # the one checked here -- rejecting every odd `*-Data.db` would red on input the
        # reader demonstrably tolerates.
        #
        # PARTLY REDUNDANT WITH THE BIDIRECTIONAL TOC CHECK, deliberately. A colliding file
        # shares the generation prefix and is not TOC-listed, so `_toc_companions_usable`
        # already disqualifies the table -- verified by deleting THIS check, which left the
        # table rejected. What it does not leave is a usable diagnostic: the operator is told
        # "no Data.db the reader would open", about a directory whose Data.db is present and
        # fine. This branch exists to NAME the cause, which is the round-40 rule, and that is
        # exactly the one assertion that flips when it is removed.
        _pfx_collide=0
        for _gen in "$cand"/*-Data.db; do
          [ -e "$_gen" ] || [ -L "$_gen" ] || continue
          _gbase=${_gen##*/}
          [ "$_gbase" = "$fbase" ] && continue
          _reader_accepts_descriptor "$_gbase" || continue
          case "$fbase" in "${_gbase%Data.db}"*) _pfx_collide=1; break ;; esac
        done
        [ "$_pfx_collide" -eq 1 ] && { collide_bad=1; gen_bad=1; }
        continue
      fi
      # `_usable_file` is `-f && -s && -r`. Test its three parts SEPARATELY so the
      # diagnostic names the operator's ACTUAL problem (roborev #3493 round 54 self-audit).
      # Collapsing them reported a Data.db that is a DIRECTORY as "ZERO-LENGTH (truncated
      # fetch?)" -- present, non-empty, and nothing to do with a truncated fetch, sending
      # the operator to re-run a fetch that was fine. That is the round-40 misattribution
      # again, reintroduced by this round's own restructure; the old code skipped these
      # shapes in silence, so the fix traded a silent miss for a wrong answer until now.
      # Three shapes, three remedies: replace the fixture / re-fetch / fix permissions.
      if [ ! -f "$f" ]; then
        type_bad=1; gen_bad=1; continue      # directory, dangling symlink, FIFO, ...
      elif [ ! -s "$f" ]; then
        empty_bad=1; gen_bad=1; continue     # regular but zero-length: a truncated fetch
      elif [ ! -r "$f" ]; then
        unread_bad=1; gen_bad=1; continue    # present and sized, but not readable
      fi
      _toc_companions_usable "$f" || { toc_bad=1; gen_bad=1; toc_why=$_TOC_FAIL_REASON; }
    done

    # ---- PASS 2: which generation satisfies THIS table's consumer -----------------------
    case "$entry" in
      test_oa/*)
        # Jest: oaBinariesPresent() = /^oa-\d+-big-Data\.db$/ -- NUMERIC only, so a
        # `oa-<uuid>-big-Data.db` is reader-supported (validated in pass 1) but does NOT
        # satisfy the OA guard. The two requirements are genuinely different and are now
        # asked separately instead of one standing in for the other.
        for f in "$cand"/*-Data.db; do
          # Plain `-e` here, deliberately: this loop picks a WINNER, and a dangling symlink
          # can never be one. Pass 1 has already reported it.
          [ -e "$f" ] || continue
          fbase=${f##*/}
          _re_match '^oa-[0-9]+-big-Data\.db$' "$fbase" || continue
          _usable_file "$f" || continue
          # The binary and the golden are INDEPENDENT in Jest, NOT a matched pair
          # (roborev #3493 round 24). oaBinariesPresent() accepts any `oa-<n>-big-Data.db`
          # and findOaJsonlFile() accepts any `oa-<n>-big-Data.db.jsonl` in the same
          # directory -- it never compares the two generations. Requiring `$f.jsonl`
          # therefore REJECTED a corpus Jest runs happily (oa-1 binary + oa-2 golden):
          # over-strict, the same false-MISSING direction as round 23.
          if _dir_has_oa_golden "$cand"; then
            [ -z "$data_db" ] && data_db="$f"
          else
            golden_bad=1
          fi
        done
        # No NUMERIC OA binary at all: distinct from "one was present but unusable", which
        # pass 1 has already recorded as empty_bad/gen_bad.
        [ -z "$data_db" ] && [ "$golden_bad" -eq 0 ] && [ "$gen_bad" -eq 0 ] && oa_bad=1 ;;
      *)
        # non-OA: the GOLDEN is always `nb-1-big-Data.db.jsonl` (findJsonlFile hard-codes
        # it), but the BINARY generation is only pinned for ONE table.
        #
        # Round 33 required a nonempty nb-1 BINARY for every non-OA table, reasoning that
        # the fixed golden name implied a fixed binary name. IT DOES NOT (roborev round
        # 34): Jest opens the table through its DIRECTORY, so any recognised generation
        # works, and the only consumer that names a binary is corrupt-fixture.js --
        # `DATA_COMPONENT = 'nb-1-big-Data.db'`, scoped to `KEYSPACE = test_basic` /
        # `TABLE = simple_table`. Applying it everywhere rejected usable
        # alternate-generation corpora: over-strict, and a gate that reds on correct input
        # is the gate agents learn to waive.
        #
        # GOLDENS ARE NONEMPTY TOO (roborev #3493 round 41). They were checked with `-f`,
        # so a TRUNCATED golden read as complete: the decision returned RUN and Jest then
        # compared real rows against an empty expectation and failed -- with the opt-out
        # unable to rescue it, because nothing had classified the corpus as incomplete.
        _need_binary=""
        [ "$entry" = "test_basic/simple_table" ] && _need_binary="nb-1-big-Data.db"
        if ! _usable_file "$cand/nb-1-big-Data.db.jsonl"; then
          # Only a directory that actually holds a generation is missing its GOLDEN; an
          # empty directory is simply not a candidate.
          _any_gen=0
          for f in "$cand"/*-Data.db; do
            [ -e "$f" ] || continue
            _reader_accepts_descriptor "${f##*/}" && _any_gen=1
          done
          [ "$_any_gen" -eq 1 ] && golden_bad=1
        elif [ -n "$_need_binary" ]; then
          # The PINNED generation must itself be usable; pass 1 has already validated every
          # OTHER generation in the directory, so this branch no longer needs to (and no
          # longer `break`s away from) that work.
          _usable_file "$cand/$_need_binary" && data_db="$cand/$_need_binary"
        else
          for f in "$cand"/*-Data.db; do
            [ -e "$f" ] || continue
            _reader_accepts_descriptor "${f##*/}" || continue
            _usable_file "$f" || continue
            [ -z "$data_db" ] && data_db="$f"
          done
        fi ;;
    esac
    # NO EARLY BREAK on the first complete candidate (roborev #3493 round 46). Jest's
    # discovery picks a table directory by readdir order and does not check usability, so
    # an EARLIER broken `<table>-<uuid>` can be the one it selects while a LATER good one
    # satisfies this check. Same "the consumer picks blind" shape as round 44's OA
    # goldens, one level up: at the directory instead of the file.
    #
    # So every committed candidate is inspected, and any unusable one disqualifies the
    # table even after a good one was found. `first_ok` remembers the good one purely for
    # the success path. Measured: no expected table has more than one candidate directory
    # on the real corpus today, so this tightens nothing that currently passes.
    # PER-CANDIDATE verdict, not per-file. A table directory may legitimately hold files
    # that are not the fixture -- a non-OA-named binary beside a valid `oa-<n>-big`, a
    # stray `junk-Data.db` beside a good `nb-1-big` -- and Jest does not care, so those
    # per-file flags are noise at this level. What matters is whether THIS CANDIDATE
    # DIRECTORY yields a usable fixture, because that is the unit Jest enumerates.
    # A reader-supported generation that is NOT usable disqualifies this candidate even
    # when a good one was also found: the reader reads them all.
    [ "$gen_bad" -eq 1 ] && data_db=""
    if [ -n "$data_db" ]; then
      [ -z "$first_ok" ] && first_ok="$data_db"
    else
      cand_bad=1     # an enumerable candidate that yields nothing usable
    fi
    data_db=""
  done
  data_db="$first_ok"
  # An unusable candidate DIRECTORY disqualifies the table even if a later one is good:
  # Jest selects by readdir order without checking usability, so it may pick the bad one.
  [ "$cand_bad" -eq 1 ] && data_db=""
  # EACH CONDITION GETS ITS OWN DIAGNOSTIC (roborev #3493 round 40). They were collapsed
  # onto `golden_bad`, so a truncated or misnamed Data.db was reported as "not the JSONL
  # golden" -- pointing the operator at a file that was present and fine. Ordered
  # most-specific first; a table can only be in one of these states per candidate.
  if [ -n "$data_db" ]; then
    :
  elif [ "$collide_bad" -eq 1 ]; then
    echo "❌ expected table has a *-Data.db that SHARES a real generation's prefix (e.g. nb-1-big-Foo-Data.db beside nb-1-big-Data.db) — the reader maps any *-Data.db to that generation's Data component and the query FAILS; remove the stray file: $entry" >&2
  elif [ "$type_bad" -eq 1 ]; then
    echo "❌ expected table has a Data.db that is NOT A REGULAR FILE (a directory, a dangling symlink, or a special file) -- replace the fixture: $entry" >&2
  elif [ "$unread_bad" -eq 1 ]; then
    echo "❌ expected table has a present, nonempty Data.db that is NOT READABLE -- check permissions: $entry" >&2
  elif [ "$empty_bad" -eq 1 ]; then
    echo "❌ expected table has a ZERO-LENGTH Data.db (truncated fetch?): $entry" >&2
  elif [ "$oa_bad" -eq 1 ]; then
    echo "❌ expected table has no OA-format Data.db (oa-<n>-big-Data.db): $entry" >&2
  elif [ "$name_bad" -eq 1 ]; then
    echo "❌ expected table has Data.db file(s) but none the reader would open (need <ver>-<id>-big|bti-Data.db, ver in na|nb|oa or da+bti): $entry" >&2
  elif [ "$golden_bad" -eq 1 ]; then
    case "$entry" in
      test_oa/*) _want="any oa-<n>-big-Data.db.jsonl in the table directory" ;;
      *)         _want="nb-1-big-Data.db.jsonl" ;;
    esac
    echo "❌ expected table has a Data.db but not the JSONL golden Jest reads ($_want): $entry" >&2
  elif [ "$toc_bad" -eq 1 ] && [ "$toc_why" = untrusted-toc ]; then
    echo "❌ expected table's TOC.txt does NOT match the git-tracked committed twin — the corpus TOC has been truncated or altered, so it cannot be trusted as the generation's inventory (a COHERENT truncation shortens the TOC in step with the files it stops listing, which every corpus-derived check accepts): $entry" >&2
  elif [ "$toc_bad" -eq 1 ]; then
    # LAST: every earlier state is a coarser breakage, and this one is only reachable
    # once a nonempty recognised binary AND its golden are both in place -- the shape a
    # partial extraction leaves.
    echo "❌ expected table's SSTable generation is incomplete: its own TOC.txt is absent, or lists a component that is missing/unreadable beside it (partial extraction?): $entry" >&2
  fi
  if [ -z "$data_db" ]; then
    echo "❌ missing Data.db for expected table: $entry" >&2
    missing=$((missing + 1))
  else
    found=$((found + 1))
  fi
done

echo "dataset manifest: ${found}/${#EXPECTED[@]} expected tables present"
if [ "$missing" -ne 0 ]; then
  echo "❌ dataset manifest check FAILED: $missing expected table(s) missing — partial extraction or dropped table?" >&2
  exit "$MANIFEST_INCOMPLETE_RC"
fi
echo "✅ dataset manifest check passed (all ${#EXPECTED[@]} expected tables present)"
