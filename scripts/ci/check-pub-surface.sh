#!/usr/bin/env bash
# check-pub-surface.sh — a STANDING PUBLIC-SURFACE SNAPSHOT GUARD for `cqlite-core`
# (issue #1712, epic #1688).
#
# # What this exists for
#
# `cqlite-core` is the crate every binding, tool and downstream consumer links
# against, and nothing in the repo noticed when an item entered or left its public
# API. The concrete instance that motivated this guard: `pub mod benchmarks;` sat
# unqualified at the crate root for months, reading to every human and every
# reviewer as a shipped public module, while a `#![cfg(feature = "benchmarks")]`
# hidden INSIDE `benchmarks/mod.rs` quietly configured it out of every default
# build. The crate root said one thing and the compiled API said another, and no
# mechanism could tell the difference.
#
# So this guard answers two questions, on every full gate:
#
#   1. Has the default-feature public API of `cqlite-core` changed since the
#      committed snapshot? (VERIFY mode diffs against `cqlite-core/pub-surface.snapshot`.)
#   2. Does the crate root TELL THE TRUTH about what it exports? (The consistency
#      assert below: an unconditional, non-hidden `pub mod NAME;` at the crate root
#      MUST be present in the default public surface.)
#
# # Where the surface comes from — rustdoc, not us
#
# The surface is read off **rustdoc's own emitted item tree**
# (`<target>/doc/cqlite_core/`), which is rustc's real name resolution and real cfg
# evaluation, already done. We never re-derive visibility from source text: a
# hand-rolled parser would be a second implementation of the compiler's rules, and
# a second implementation's correctness is only knowable by differential testing
# against the original (CLAUDE.md, #3283). Reading rustdoc's output has no such
# problem — it IS the original.
#
# The one thing rustdoc cannot tell us is `#[doc(hidden)]` items, which it omits by
# construction. Those are therefore recorded in a second, source-derived section
# covering ONLY the crate-root declarations of `cqlite-core/src/lib.rs` — enough to
# pin the declaration-site shape (`#[cfg(...)]`, `#[doc(hidden)]`) of every
# top-level `pub mod` / `pub use`, which is exactly what the consistency assert
# needs and exactly what the #1712 defect hid.
#
# # No invoker-selectable subject
#
# The target package and the snapshot path are HARD-CODED. No flag and no
# environment variable may select them, and none may be added. A gate component
# whose subject its invoker can choose can be pointed at a trivial subject and
# greened vacuously — the same reason `scripts/flow/roborev-review-oracles.sh`
# resolves its enforcer from its own directory with no override (CLAUDE.md: "the
# constrained party must not choose its own enforcer"). A test that needs a
# different subject SUBSTITUTES THE ARTIFACT in its own scratch checkout
# (`git worktree add --detach`), which is what scripts/tests/test_pub_surface_guard.sh
# does.
#
# # Fail-closed, affirmatively
#
# A positive verdict requires an affirmative measurement (CLAUDE.md). There is no
# path through this script on which "nothing was measured" reads as PASS:
# `cargo doc` failing, the doc tree being absent, zero items enumerated, or the
# committed snapshot being unreadable are each a NAMED FAIL. There is no opt-out.
#
# # Stated boundary (do not overclaim)
#
# Granularity is item PATHS, KINDS and associated-item NAMES — never SIGNATURES.
# It catches an added, removed or renamed public item, method, enum variant,
# public field or associated const/type. It does NOT catch a changed parameter
# type, a changed return type, changed generics or bounds, or a changed field
# TYPE, and it deliberately does not record trait/synthetic/blanket impl members
# (see the associated-item pass for why). It is a coarse semver tripwire, not a
# semver checker (`cargo-public-api` would be the latter, but it is nightly-only
# and this repo pins stable).
#
# Exit 0 = surface matches + crate root is consistent. 1 = drift/inconsistency.
# 2 = usage error.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Hard-coded subject — see "No invoker-selectable subject" above. Deliberately not
# parameterised, not env-overridable.
readonly PACKAGE="cqlite-core"
readonly CRATE_DOC_NAME="cqlite_core"
readonly SNAPSHOT_REL="cqlite-core/pub-surface.snapshot"
readonly LIB_RS_REL="cqlite-core/src/lib.rs"

SNAPSHOT="$REPO_ROOT/$SNAPSHOT_REL"
LIB_RS="$REPO_ROOT/$LIB_RS_REL"

MODE=verify

usage() {
  cat <<'EOF'
Usage: scripts/ci/check-pub-surface.sh [--regenerate|--write] [--help]

Public-surface snapshot guard for the cqlite-core crate (issue #1712).

  (no flags)      VERIFY: derive the default-feature public surface from rustdoc
                  and diff it against the committed cqlite-core/pub-surface.snapshot.
                  Also asserts the crate root tells the truth about its exports.
  --regenerate    Rewrite cqlite-core/pub-surface.snapshot from the current tree.
  --write         Synonym for --regenerate.
  --help          This message.

The target package and snapshot path are hard-coded and cannot be selected by any
flag or environment variable, deliberately: a gate component must not let its
invoker point it at a trivial subject and pass vacuously.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --regenerate|--write) MODE=regenerate ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "check-pub-surface.sh: unrecognized argument '$1'" >&2
      echo "" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

fail() {
  echo "" >&2
  echo "❌ pub-surface: $*" >&2
  exit 1
}

# Shared paths + scratch space for every step below.
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
DOC_ROOT="$TARGET_DIR/doc/$CRATE_DOC_NAME"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface.XXXXXX")"
cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1) Scan the CRATE-ROOT declarations of cqlite-core/src/lib.rs.
#
#    Deliberately FIRST, before the docs are built: it is independent of rustdoc,
#    and a crate root the guard cannot parse is a verdict-blocking condition. Doing
#    it here means that failure is reported in well under a second instead of after
#    a doc build whose result could not be used anyway.
#
#    Only the crate root, not the tree: this section exists so that declaration-site
#    attributes (`#[cfg(...)]`, `#[doc(hidden)]`) — which rustdoc cannot show us for
#    hidden or configured-out items — are recorded somewhere, and so the consistency
#    assert below has something to compare against.
#
#    THIS IS A LEXICAL SCAN OF ONE FILE WITH A PINNED EDGE-CASE SUITE — NOT A RUST
#    PARSER, and it does not pretend to be one. A hand-written parser is a second
#    implementation of the language, and a second implementation's correctness is
#    only knowable by differential testing against the original (CLAUDE.md, #3283).
#    What makes this safe is not soundness, it is (a) the fail-safe split below and
#    (b) `scripts/tests/test_pub_surface_guard.sh`, which pins every shape this scan
#    is known to handle — plain, attribute-on-its-own-line, SAME-LINE
#    `#[attr] pub mod x;`, MULTI-LINE attributes, a trailing `// comment`, a
#    `pub mod` inside a `/* */` block, and `#[doc(hidden)]`.
#
#    THE FAIL-SAFE SPLIT (this is the important part). Two different questions are
#    answered by two independent derivations:
#
#      S — "which modules are declared at the crate root?" answered by the SIMPLEST
#          rule that can be written, with no attribute parsing at all: any
#          comment-stripped, column-zero line containing `pub mod NAME;`.
#      P — the structured scan, which additionally attaches each declaration's
#          attributes (joining multi-line attributes by bracket balance and
#          splitting a same-line `#[attr] item`).
#
#    If S and P disagree about the SET of crate-root modules, the scan cannot be
#    trusted and the guard FAILs — it never silently proceeds on the smaller set.
#    Under-collecting here is precisely what produced the original false PASS: an
#    `#[cfg(...)] pub mod x;` written on ONE line was dropped entirely, so that
#    module escaped the consistency assert AND vanished from the snapshot.
#
#    Where P cannot determine an item's attributes it yields NONE, which lands the
#    module in the ASSERTED set. A parser miss therefore reds loudly instead of
#    waving a module through.
# ---------------------------------------------------------------------------
[ -f "$LIB_RS" ] || fail "$LIB_RS_REL not found — cannot scan the crate-root declarations."

CRATEROOT_AWK="$WORK_DIR/crateroot.awk"
cat >"$CRATEROOT_AWK" <<'CRATEROOT_AWK_EOF'
# Lexical scan of a crate root. NOT a Rust parser — see the guard's comment block.
{ L[NR] = $0 }
function ltrim(x) { sub(/^[[:space:]]+/, "", x); return x }
function rtrim(x) { sub(/[[:space:]]+$/, "", x); return x }
function squash(x) { gsub(/[[:space:]]+/, " ", x); return ltrim(rtrim(x)) }

# Blank out comments, preserving line structure and column positions, and record
# for each line whether it STARTS in ordinary code (a line that begins inside a
# string or a block comment can never open a crate-root declaration).
function normalize(   i, s, out, j, c, c2, st, depth, hashes, k, cnt) {
  st = "code"; depth = 0; hashes = 0
  for (i = 1; i <= n; i++) {
    INCODE[i] = (st == "code") ? 1 : 0
    s = L[i]; out = ""; j = 1
    while (j <= length(s)) {
      c = substr(s, j, 1); c2 = substr(s, j, 2)
      if (st == "code") {
        if (c2 == "//") { out = out "  "; j += 2; st = "line"; continue }
        if (c2 == "/*") { out = out "  "; j += 2; depth = 1; st = "block"; continue }
        if (c == "\"") { out = out c; j++; st = "str"; continue }
        if (c == "r" && (substr(s, j+1, 1) == "\"" || substr(s, j+1, 1) == "#")) {
          k = j + 1; hashes = 0
          while (substr(s, k, 1) == "#") { hashes++; k++ }
          if (substr(s, k, 1) == "\"") { out = out substr(s, j, k - j + 1); j = k + 1; st = "raw"; continue }
        }
        out = out c; j++
      } else if (st == "line") {
        out = out " "; j++
      } else if (st == "block") {
        if (c2 == "/*") { depth++; out = out "  "; j += 2; continue }
        if (c2 == "*/") { depth--; out = out "  "; j += 2; if (depth == 0) st = "code"; continue }
        out = out " "; j++
      } else if (st == "str") {
        if (c == "\\") { out = out "  "; j += 2; continue }
        out = out c; j++
        if (c == "\"") st = "code"
      } else if (st == "raw") {
        if (c == "\"") {
          k = j + 1; cnt = 0
          while (substr(s, k, 1) == "#" && cnt < hashes) { cnt++; k++ }
          if (cnt == hashes) { out = out substr(s, j, k - j); j = k; st = "code"; continue }
        }
        out = out c; j++
      }
      # `st == "line"` ends with the line; block/str/raw carry over deliberately.
    }
    if (st == "line") st = "code"
    N[i] = rtrim(out)
  }
  if (st != "code") print "E\tcrate root ends inside a " st " (unterminated comment or string literal)"
}

# Consume one `#[...]` / `#![...]` attribute from the front of BUF, appending
# following lines until its brackets balance. Returns "" if it never balances.
function take_attr(   d, p, ch, res, instr) {
  d = 0; p = 1; instr = 0
  while (1) {
    if (p > length(BUF)) {
      CUR++
      if (CUR > n) return ""
      BUF = BUF " " ltrim(N[CUR])
      continue
    }
    ch = substr(BUF, p, 1)
    if (instr) {
      if (ch == "\\") p++
      else if (ch == "\"") instr = 0
    } else if (ch == "\"") instr = 1
    else if (ch == "[") d++
    else if (ch == "]") {
      d--
      if (d == 0) { res = substr(BUF, 1, p); BUF = ltrim(substr(BUF, p + 1)); return res }
    }
    p++
  }
}

# Consume through the terminating `;` of a statement, appending lines as needed.
function take_stmt(   p, ch, res, instr) {
  p = 1; instr = 0
  while (1) {
    if (p > length(BUF)) {
      CUR++
      if (CUR > n) return ""
      BUF = BUF " " ltrim(N[CUR])
      continue
    }
    ch = substr(BUF, p, 1)
    if (instr) {
      if (ch == "\\") p++
      else if (ch == "\"") instr = 0
    } else if (ch == "\"") instr = 1
    else if (ch == ";") { res = substr(BUF, 1, p); BUF = ltrim(substr(BUF, p + 1)); return res }
    p++
  }
}

END {
  n = NR
  normalize()

  # --- Derivation S: "which modules are declared at the crate root?" answered by
  # the simplest rule that can be written, independent of all attribute parsing.
  for (i = 1; i <= n; i++) {
    if (!INCODE[i]) continue
    t = N[i]
    if (t ~ /^[[:space:]]/ || t == "") continue
    # EVERY occurrence on the line, not just the first: two declarations sharing a
    # line is exotic, but if S found only the first it would AGREE with a structured
    # scan that also stops at the first, and the pair would silently under-collect.
    # Finding all of them turns that shape into a loud cross-check FAIL instead.
    rest = t
    while (match(rest, /pub mod [A-Za-z_][A-Za-z0-9_]*[[:space:]]*;/)) {
      nm = substr(rest, RSTART + 8, RLENGTH - 8)
      sub(/[[:space:]]*;$/, "", nm)
      print "S\t" nm
      rest = substr(rest, RSTART + RLENGTH)
    }
  }

  # --- Derivation P: the structured scan (attributes joined, same-line splits).
  CUR = 1
  while (CUR <= n) {
    if (!INCODE[CUR]) { CUR++; continue }
    line = N[CUR]
    if (line == "" || line ~ /^[[:space:]]/) { CUR++; continue }
    startline = CUR
    BUF = line
    attrs = ""
    # Consume the run of OUTER attributes preceding an item. They may each span
    # several lines, they may sit on their own lines, and the item may share the
    # last attribute's line — all three shapes converge here.
    while (1) {
      if (BUF == "") {
        if (CUR >= n) break
        if (!INCODE[CUR + 1]) break
        if (N[CUR + 1] == "" || N[CUR + 1] ~ /^[[:space:]]/) break
        CUR++
        BUF = N[CUR]
      }
      if (BUF !~ /^#!?\[/) break
      a = take_attr()
      if (a == "") { print "E\tunterminated attribute starting at line " startline; BUF = ""; break }
      # An INNER attribute (`#![...]`) belongs to the enclosing module, not to a
      # following item; it must not accumulate onto one.
      if (a ~ /^#!\[/) attrs = ""
      else attrs = attrs squash(a) " "
    }
    if (BUF ~ /^pub mod [A-Za-z_][A-Za-z0-9_]*[[:space:]]*;/) {
      st2 = take_stmt()
      nm = st2
      sub(/^pub mod[[:space:]]+/, "", nm)
      sub(/[[:space:]]*;$/, "", nm)
      printf "M\t%d\t%s\t%s\n", CUR, nm, squash(attrs)
      printf "D\t%d\t%s%s\n", startline, attrs, squash(st2)
    } else if (BUF ~ /^pub use[[:space:]]/) {
      st2 = take_stmt()
      if (st2 == "") {
        print "E\tunterminated `pub use` starting at line " startline
      } else {
        printf "D\t%d\t%s%s\n", startline, attrs, squash(st2)
      }
    }
    CUR++
  }
}
CRATEROOT_AWK_EOF

SCAN_RAW="$WORK_DIR/crateroot.txt"
awk -f "$CRATEROOT_AWK" "$LIB_RS" >"$SCAN_RAW"

# The record tag is separated by a LITERAL TAB in the patterns below (not a `\t`
# escape, which POSIX grep reads as a plain `t` — a selector that silently matched
# nothing would turn each of these fail-closed checks into a vacuous pass).

if grep -q '^E	' "$SCAN_RAW"; then
  echo "" >&2
  grep '^E	' "$SCAN_RAW" | cut -f2- >&2
  fail "the crate-root lexical scan of $LIB_RS_REL hit input it could not read (see above). Refusing to report a verdict over a crate root it could not fully parse."
fi

DERIVED_DECLS="$WORK_DIR/decls.txt"
DERIVED_MODS="$WORK_DIR/mods.txt"
grep '^D	' "$SCAN_RAW" | cut -f2- >"$DERIVED_DECLS" || true
grep '^M	' "$SCAN_RAW" | cut -f2- >"$DERIVED_MODS" || true

# Cross-check the two derivations of "which modules are declared at the crate root".
grep '^S	' "$SCAN_RAW" | cut -f2- | LC_ALL=C sort -u >"$WORK_DIR/mods.simple"
cut -f2 "$DERIVED_MODS" | LC_ALL=C sort -u >"$WORK_DIR/mods.structured"
if ! diff -u "$WORK_DIR/mods.simple" "$WORK_DIR/mods.structured" >"$WORK_DIR/mods.diff" 2>&1; then
  echo "" >&2
  echo "simple-scan vs structured-scan module sets:" >&2
  sed -e '1s|.*|--- simple scan|' -e '2s|.*|+++ structured scan|' "$WORK_DIR/mods.diff" >&2
  fail "the two independent scans of $LIB_RS_REL disagree about which modules the crate root declares. One of them is wrong, so neither result can be trusted — the guard refuses to assert over a module set it cannot pin down. (This is the guard that would have caught the same-line \`#[attr] pub mod x;\` shape being dropped.)"
fi

DECL_COUNT="$(wc -l <"$DERIVED_DECLS" | tr -d ' ')"
[ "${DECL_COUNT:-0}" -gt 0 ] || fail "no crate-root \`pub mod\`/\`pub use\` declarations found in $LIB_RS_REL — the scan measured nothing. Refusing to pass."
MOD_COUNT="$(wc -l <"$DERIVED_MODS" | tr -d ' ')"
[ "${MOD_COUNT:-0}" -gt 0 ] || fail "no crate-root \`pub mod\` declarations found in $LIB_RS_REL — the scan measured nothing. Refusing to pass."


# In VERIFY mode the committed snapshot is the baseline the whole run exists to
# compare against, so its absence is checked HERE rather than after the doc build:
# there is no point spending a rustdoc build to report a missing baseline. A missing
# snapshot is a FAIL, never an implicit pass.
if [ "$MODE" = verify ] && [ ! -r "$SNAPSHOT" ]; then
  fail "committed snapshot $SNAPSHOT_REL is MISSING or unreadable. It is required — a missing snapshot is a FAIL, never an implicit pass. Create it with:
       bash scripts/ci/check-pub-surface.sh --regenerate"
fi

# ---------------------------------------------------------------------------
# 2) Build the docs (default features) and locate the emitted item tree.
# ---------------------------------------------------------------------------
DOC_LOG="$WORK_DIR/cargo-doc.log"

# Remove the previous emission first: a doc tree left behind by an earlier run with
# DIFFERENT features (e.g. `--features benchmarks`) would otherwise contribute stale
# directories to the enumeration, and the snapshot would record a surface no default
# build has. rustdoc regenerates it in a few seconds.
rm -rf "$DOC_ROOT"

if ! (cd "$REPO_ROOT" && cargo doc --no-deps --quiet --package "$PACKAGE" --lib) >"$DOC_LOG" 2>&1; then
  echo "--- last 40 lines of cargo doc output ---" >&2
  tail -40 "$DOC_LOG" >&2
  echo "--- end ---" >&2
  fail "\`cargo doc --no-deps --package $PACKAGE --lib\` FAILED. The public surface could not be measured, so this check reports FAIL — never a vacuous pass."
fi

if [ ! -d "$DOC_ROOT" ]; then
  fail "cargo doc succeeded but the emitted item tree $DOC_ROOT is ABSENT. Nothing could be enumerated; refusing to report a pass over an unmeasured surface."
fi

# ---------------------------------------------------------------------------
# 3) Enumerate the rustdoc item tree -> "<kind> cqlite_core::a::b::Name" lines.
#
#    One directory per public module; one `<kind>.<Name>.html` file per public
#    item; and, per page, the ASSOCIATED items declared by this crate (inherent
#    methods, associated consts/types, enum variants, public struct fields, trait
#    required/provided members). rustdoc's own non-item files are skipped by name.
# ---------------------------------------------------------------------------
DERIVED_ITEMS="$WORK_DIR/items.txt"
PAGES="$WORK_DIR/pages.txt"
: >"$DERIVED_ITEMS"
: >"$PAGES"

# Modules: every directory below the crate doc root, plus the crate root itself.
{
  printf 'mod %s\n' "$CRATE_DOC_NAME"
  find "$DOC_ROOT" -mindepth 1 -type d -print | while IFS= read -r d; do
    rel="${d#"$DOC_ROOT"/}"
    case "$rel" in
      # rustdoc's own asset/aggregate trees. They live beside the crate dir rather
      # than inside it on current stable, but skipping them here keeps the
      # enumeration correct if that ever changes.
      static.files|static.files/*|src|src/*|trait.impl|trait.impl/*|type.impl|type.impl/*|implementors|implementors/*) continue ;;
    esac
    printf 'mod %s::%s\n' "$CRATE_DOC_NAME" "${rel//\//::}"
  done
} >>"$DERIVED_ITEMS"

# Items: `<kind>.<Name>.html` for every kind rustdoc emits as a standalone page.
find "$DOC_ROOT" -type f -name '*.html' -print | while IFS= read -r f; do
  rel="${f#"$DOC_ROOT"/}"
  base="${rel##*/}"
  dir="${rel%/*}"
  [ "$dir" = "$rel" ] && dir=""
  case "$base" in
    index.html|all.html|help.html|settings.html) continue ;;
  esac
  kind="${base%%.*}"
  case "$kind" in
    struct|enum|fn|trait|type|constant|macro|union|derive|attr|primitive|static|mod) ;;
    *) continue ;;
  esac
  name="${base#"$kind".}"
  name="${name%.html}"
  # Record the page for the associated-item pass below (same subject set, one
  # enumeration decision, so the two passes can never disagree about what an
  # "item page" is).
  printf '%s\n' "$f" >>"$PAGES"
  if [ -n "$dir" ]; then
    printf '%s %s::%s::%s\n' "$kind" "$CRATE_DOC_NAME" "${dir//\//::}" "$name"
  else
    printf '%s %s::%s\n' "$kind" "$CRATE_DOC_NAME" "$name"
  fi
done >>"$DERIVED_ITEMS"

# --- Associated items -------------------------------------------------------
#
# Standalone rustdoc pages alone are BLIND to public methods, enum variants,
# public struct fields and associated consts/types: adding a `pub fn` to an
# existing public struct would not move the snapshot at all. So each item page is
# also scanned for the associated items THIS CRATE DECLARES.
#
# WHAT IS DELIBERATELY EXCLUDED, and why: the `trait-implementations`,
# `synthetic-implementations`, `blanket-implementations`, `implementors`,
# `foreign-impls` and `deref-methods-*` sections. Those anchors are not this
# crate's declared surface — they are generated from trait impls, auto traits and
# blanket impls in dependencies (`struct.DatabaseStats.html` carries 17
# `id="method.*"` anchors and ZERO inherent methods; every one comes from Clone,
# Debug, Into, TryFrom, Borrow, tracing's Instrument, …). They move whenever a
# dependency is bumped, so recording them would make this snapshot a churn source
# and train people to regenerate it without reading the diff — the failure mode
# this guard exists to prevent. The exclusion is stated in the snapshot header
# too, as an honest boundary rather than a silent gap.
#
# The scan is SECTION-SCOPED, and the scoping is done WITHIN each line, not per
# line: rustdoc's HTML is near-minified (a whole page is ~17 lines) and a single
# line routinely carries the end of one section header and the start of the next,
# so a line-granular state machine would attribute trait-impl anchors to the
# inherent `implementations` section. A real section boundary is recognised by
# `<h2 id="…" class="…section-header">`; a doc-comment heading inside a docblock
# (`<h2 id="safety">`, `<h2 id="note">`, … — no class) must NOT move the state.
ASSOC_AWK="$WORK_DIR/assoc.awk"
cat >"$ASSOC_AWK" <<'AWK_EOF'
BEGIN {
  # Sections whose anchors ARE this crate's declared surface.
  split("variants fields implementations required-methods provided-methods required-associated-consts provided-associated-consts required-associated-types provided-associated-types", _w, " ")
  for (_k in _w) if (_w[_k] != "") WANT[_w[_k]] = 1
  prefix = docroot "/"
}
FNR == 1 {
  rel = FILENAME
  if (substr(rel, 1, length(prefix)) == prefix) rel = substr(rel, length(prefix) + 1)
  base = rel
  sub(/^.*\//, "", base)
  dir = rel
  if (dir == base) { dir = "" } else { sub(/\/[^\/]*$/, "", dir) }
  pkind = base
  sub(/\..*$/, "", pkind)
  iname = substr(base, length(pkind) + 2)
  sub(/\.html$/, "", iname)
  gsub(/\//, "::", dir)
  itempath = (dir == "" ? crate : crate "::" dir) "::" iname
  section = ""
}
{
  n = split($0, seg, "<h2 ")
  for (_s = 1; _s <= n; _s++) {
    piece = seg[_s]
    if (_s > 1) {
      if (match(piece, /^id="[A-Za-z0-9_-]+" class="[^"]*section-header"/)) {
        hdr = substr(piece, RSTART, RLENGTH)
        sub(/^id="/, "", hdr)
        sub(/".*$/, "", hdr)
        section = hdr
        if (section in WANT) seen[FILENAME "\t" section] = 1
      }
      # else: a doc-comment heading inside a docblock — state must NOT move.
    }
    if (!(section in WANT)) continue
    rest = piece
    while (match(rest, /id="(method|tymethod|variant|structfield|associatedconstant|associatedtype)\.[A-Za-z0-9_]+(\.field\.[A-Za-z0-9_]+)?"/)) {
      tok = substr(rest, RSTART + 4, RLENGTH - 5)
      rest = substr(rest, RSTART + RLENGTH)
      akind = tok
      sub(/\..*$/, "", akind)
      aname = substr(tok, length(akind) + 2)
      if (index(aname, ".field.") > 0) {
        sub(/\.field\./, "::", aname)
        akind = "variantfield"
      }
      print akind " " itempath "::" aname
      filled[FILENAME "\t" section] = 1
    }
  }
}
END {
  # A wanted section that was PRESENT but yielded nothing is a measurement
  # failure (rustdoc anchor format changed), not an empty section. Report it;
  # the caller turns it into a named FAIL.
  for (key in seen) if (!(key in filled)) print "!EMPTY\t" key > "/dev/stderr"
}
AWK_EOF

ASSOC_RAW="$WORK_DIR/assoc.txt"
ASSOC_ERR="$WORK_DIR/assoc.err"
if [ -s "$PAGES" ]; then
  tr '\n' '\0' <"$PAGES" | xargs -0 awk -v crate="$CRATE_DOC_NAME" -v docroot="$DOC_ROOT" \
    -f "$ASSOC_AWK" >"$ASSOC_RAW" 2>"$ASSOC_ERR"
else
  : >"$ASSOC_RAW"; : >"$ASSOC_ERR"
fi

if [ -s "$ASSOC_ERR" ]; then
  echo "" >&2
  echo "Sections present but yielding no anchors (first 10):" >&2
  head -10 "$ASSOC_ERR" >&2
  fail "the associated-item scan found rustdoc sections it could not read. That is the signature of a rustdoc HTML format change, not an empty API — refusing to record a surface measured with a broken extractor."
fi

ASSOC_COUNT="$(wc -l <"$ASSOC_RAW" | tr -d ' ')"
if [ "${ASSOC_COUNT:-0}" -eq 0 ]; then
  fail "the associated-item scan extracted ZERO methods/variants/fields/associated items across the entire crate. At this crate's size that is implausible — it is the signature of a rustdoc HTML format change. Refusing to pass on an unmeasured surface."
fi
cat "$ASSOC_RAW" >>"$DERIVED_ITEMS"

LC_ALL=C sort -o "$DERIVED_ITEMS" "$DERIVED_ITEMS"

MODULE_COUNT="$(grep -c '^mod ' "$DERIVED_ITEMS" || true)"
ITEM_COUNT="$(grep -vc '^mod ' "$DERIVED_ITEMS" || true)"
ITEM_COUNT=$((ITEM_COUNT - ASSOC_COUNT))

if [ "${ITEM_COUNT:-0}" -eq 0 ] || [ "${MODULE_COUNT:-0}" -eq 0 ]; then
  fail "enumerated $ITEM_COUNT items over $MODULE_COUNT modules under $DOC_ROOT — a zero count means the enumeration did not measure anything (rustdoc layout changed?), NOT that the crate has no public API. Refusing to pass."
fi

# ---------------------------------------------------------------------------
# 4) THE CONSISTENCY ASSERT (the core of #1712).
#
#    Every crate-root `pub mod NAME;` that carries NO declaration-site `#[cfg(...)]`
#    and is NOT `#[doc(hidden)]` reads to every human as an unconditional public
#    export. It MUST therefore be present in the default-feature public surface. If
#    it is not, its gate is hiding inside the module file where no reader of the
#    crate root can see it — exactly the #1712 defect.
# ---------------------------------------------------------------------------
inconsistent=0
while IFS=$'\t' read -r lineno modname attrs; do
  # Which declarations are EXEMPT from the assert, and why each one is:
  #
  #   * `#[cfg(...)]` at the declaration site — the gate is visible to every reader
  #     of the crate root, which is exactly what this assert wants.
  #   * anything applying `doc(hidden)` (directly or through a `cfg_attr`) — the item
  #     is deliberately undocumented, so rustdoc omitting it proves nothing.
  #   * a `cfg_attr` that could itself APPLY a `cfg` — under some configuration it
  #     becomes a declaration-site gate, so its absence from the surface may be
  #     legitimate.
  #
  # A `cfg_attr` that can apply NEITHER is NOT exempt. Treating every `cfg_attr` as
  # an exemption (as the first cut did) reopened the exact bypass this assert exists
  # to close: a purely cosmetic `#[cfg_attr(docsrs, doc(alias = "…"))]` would let a
  # module keep hiding its real gate inside its own file.
  case "$attrs" in
    *'#[cfg('*|*'doc(hidden)'*) continue ;;
  esac
  case "$attrs" in
    *'#[cfg_attr('*)
      # Everything from the first `cfg_attr` on — deliberately over-approximate
      # (a later, unrelated attribute is included), because over-approximating
      # here only ever SKIPS a check, never invents a failure.
      cfg_attr_body="${attrs#*'#[cfg_attr('}"
      # `doc(cfg(...))` is the docs.rs "shown as feature-gated" annotation. It
      # contains the token `cfg(` but applies no gate at all, so it must not be
      # mistaken for one; neutralise it before testing.
      cfg_attr_body="${cfg_attr_body//doc(cfg(/doc(}"
      case "$cfg_attr_body" in
        *'cfg('*) continue ;;
      esac
      ;;
  esac
  if ! grep -qx "mod $CRATE_DOC_NAME::$modname" "$DERIVED_ITEMS"; then
    inconsistent=$((inconsistent + 1))
    echo "" >&2
    echo "❌ pub-surface: crate-root declaration is INCONSISTENT with the real public surface" >&2
    echo "" >&2
    echo "    \`pub mod $modname\` at $LIB_RS_REL:$lineno reads as an unconditional public" >&2
    echo "    export, but it is ABSENT from the default-feature public surface." >&2
    echo "" >&2
    echo "    Its cfg gate lives INSIDE the module file (an inner \`#![cfg(...)]\`), where no" >&2
    echo "    reader of the crate root can see it: the crate root says the module ships, the" >&2
    echo "    compiled API says it does not." >&2
    echo "" >&2
    echo "    Remedy — hoist the gate to the declaration site so the crate root tells the truth:" >&2
    echo "        #[cfg(feature = \"…\")]" >&2
    echo "        pub mod $modname;" >&2
    echo "    …or, if it is deliberately an undocumented internal surface, mark it" >&2
    echo "        #[doc(hidden)]" >&2
    echo "        pub mod $modname;" >&2
  fi
done <"$DERIVED_MODS"

[ "$inconsistent" -eq 0 ] || fail "$inconsistent crate-root declaration(s) disagree with the default public surface (see above). Issue #1712."

# ---------------------------------------------------------------------------
# 5) Render the snapshot and either write it or diff it.
# ---------------------------------------------------------------------------
RENDERED="$WORK_DIR/rendered.snapshot"
{
  cat <<EOF
# cqlite-core public-surface snapshot (issue #1712)
#
# WHAT THIS IS: the default-feature public API of the \`cqlite-core\` crate, derived
# from rustdoc's own emitted item tree (rustc's real name resolution and real cfg
# evaluation — never re-derived from source text by us).
#
# REGENERATE WITH:  bash scripts/ci/check-pub-surface.sh --regenerate
# VERIFIED BY:      the \`pub-surface\` component of scripts/agent-gate.sh
#
# A DIFF HERE IS A PUBLIC-API CHANGE AND REQUIRES REVIEW. It is a semver decision,
# not a formatting chore: an added line is new public surface this crate must now
# keep; a removed or renamed line is a breaking change for every binding, tool and
# downstream consumer. Regenerate it in the SAME commit as the API change, and say
# in the PR body why the diff is what it is.
#
# STATED BOUNDARY (deliberately narrow — do not read more into a green than this):
#   * Granularity is item PATHS, KINDS and associated-item NAMES — never SIGNATURES.
#     Recorded: modules, standalone items (struct/enum/fn/trait/type/constant/macro/
#     union/…), and, per item, the associated members this crate DECLARES — inherent
#     methods, associated consts/types, enum variants (and their struct fields),
#     public struct fields, and a trait's required/provided members.
#     NOT recorded, and therefore NOT detected: a changed parameter type, a changed
#     return type, changed generics or bounds, a changed field TYPE, or a changed
#     visibility that does not change the item's presence.
#   * TRAIT / SYNTHETIC / BLANKET IMPL MEMBERS ARE DELIBERATELY EXCLUDED, as are
#     \`deref-methods-*\` sections. Those anchors come from impls of foreign traits and
#     from auto/blanket impls in dependencies — they are not this crate's declared
#     surface, and they move on any dependency bump, which would turn this file into a
#     churn source and train reviewers to regenerate it without reading the diff. So a
#     newly IMPLEMENTED foreign trait does not show up here. That is a real gap, stated
#     rather than hidden.
#   * \`#[doc(hidden)]\` items are invisible to rustdoc and therefore appear ONLY in
#     the crate-root-declarations section below, and only if they are declared at
#     the crate root.
#   * The surface is measured with DEFAULT features only. Items behind non-default
#     features (\`benchmarks\`, \`parquet\`, \`cli-helpers\`, \`fuzz\`, …) are absent here
#     by design.
#
# Sections: rustdoc items first, then the crate-root declarations of
# cqlite-core/src/lib.rs recorded WITH their declaration-site attributes (which is
# what the guard's crate-root consistency assert compares against).

## rustdoc-public-surface (default features)
EOF
  cat "$DERIVED_ITEMS"
  echo ""
  echo "## crate-root-declarations ($LIB_RS_REL)"
  cut -f2- <"$DERIVED_DECLS"
} >"$RENDERED"

if [ "$MODE" = regenerate ]; then
  cp "$RENDERED" "$SNAPSHOT"
  echo "pub-surface: WROTE $SNAPSHOT_REL — $ITEM_COUNT public items + $ASSOC_COUNT associated items over $MODULE_COUNT modules; $DECL_COUNT crate-root declarations."
  echo "             Review the diff: it is a public-API change, not a formatting chore."
  exit 0
fi

if ! diff -u "$SNAPSHOT" "$RENDERED" >"$WORK_DIR/surface.diff" 2>&1; then
  echo "" >&2
  echo "❌ pub-surface: the public API of $PACKAGE has CHANGED relative to the committed snapshot." >&2
  echo "" >&2
  echo "--- diff: $SNAPSHOT_REL (committed)  vs  freshly derived ---" >&2
  sed -e "1s|.*|--- $SNAPSHOT_REL (committed)|" -e "2s|.*|+++ freshly derived from rustdoc|" "$WORK_DIR/surface.diff" >&2
  echo "--- end of diff ---" >&2
  echo "" >&2
  echo "    If this change to the public API is INTENDED, regenerate the snapshot in the" >&2
  echo "    same commit and explain the diff in the PR body:" >&2
  echo "        bash scripts/ci/check-pub-surface.sh --regenerate" >&2
  echo "" >&2
  echo "    If it is NOT intended, you have accidentally added, removed or renamed a public" >&2
  echo "    item of $PACKAGE — a semver-relevant change for every binding and downstream" >&2
  echo "    consumer. Fix the code, not the snapshot." >&2
  exit 1
fi

# Affirmative success line: a pasted gate SUMMARY must show that this check RAN.
echo "pub-surface: $ITEM_COUNT public items + $ASSOC_COUNT associated items (methods/variants/fields/assoc consts) over $MODULE_COUNT modules match $SNAPSHOT_REL; $DECL_COUNT crate-root declarations consistent"
