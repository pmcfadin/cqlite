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
# Granularity is item PATHS and KINDS, not signatures. This catches an added,
# removed or renamed public item; it does NOT catch a changed parameter type, a
# changed return type, a new trait impl, or a changed field. It is a coarse
# semver tripwire, not a semver checker (`cargo-public-api` would be the latter,
# but it is nightly-only and this repo pins stable).
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

# ---------------------------------------------------------------------------
# 1) Build the docs (default features) and locate the emitted item tree.
# ---------------------------------------------------------------------------
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
DOC_ROOT="$TARGET_DIR/doc/$CRATE_DOC_NAME"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface.XXXXXX")"
cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

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
# 2) Enumerate the rustdoc item tree -> "<kind> cqlite_core::a::b::Name" lines.
#
#    One directory per public module; one `<kind>.<Name>.html` file per public
#    item. rustdoc's own non-item files are skipped by name.
# ---------------------------------------------------------------------------
DERIVED_ITEMS="$WORK_DIR/items.txt"
: >"$DERIVED_ITEMS"

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
  if [ -n "$dir" ]; then
    printf '%s %s::%s::%s\n' "$kind" "$CRATE_DOC_NAME" "${dir//\//::}" "$name"
  else
    printf '%s %s::%s\n' "$kind" "$CRATE_DOC_NAME" "$name"
  fi
done >>"$DERIVED_ITEMS"

LC_ALL=C sort -o "$DERIVED_ITEMS" "$DERIVED_ITEMS"

MODULE_COUNT="$(grep -c '^mod ' "$DERIVED_ITEMS" || true)"
ITEM_COUNT="$(grep -vc '^mod ' "$DERIVED_ITEMS" || true)"

if [ "${ITEM_COUNT:-0}" -eq 0 ] || [ "${MODULE_COUNT:-0}" -eq 0 ]; then
  fail "enumerated $ITEM_COUNT items over $MODULE_COUNT modules under $DOC_ROOT — a zero count means the enumeration did not measure anything (rustdoc layout changed?), NOT that the crate has no public API. Refusing to pass."
fi

# ---------------------------------------------------------------------------
# 3) Scan the CRATE-ROOT declarations of cqlite-core/src/lib.rs.
#
#    Only the crate root, not the tree: this section exists so that declaration-site
#    attributes (`#[cfg(...)]`, `#[doc(hidden)]`) — which rustdoc cannot show us for
#    hidden or configured-out items — are recorded somewhere, and so the consistency
#    assert below has something to compare against.
# ---------------------------------------------------------------------------
[ -f "$LIB_RS" ] || fail "$LIB_RS_REL not found — cannot scan the crate-root declarations."

DERIVED_DECLS="$WORK_DIR/decls.txt"
awk '
  function flush_attrs() { attrs = ""; }
  # Accumulate attributes; they apply to the next item only.
  /^#\[/ { attrs = attrs $0 " "; next }
  # `pub mod NAME;`
  /^pub mod [A-Za-z_][A-Za-z0-9_]*;/ {
    print line_prefix() attrs $0;
    flush_attrs(); next
  }
  # `pub use ...;` possibly spanning multiple lines.
  /^pub use / {
    stmt = $0; ln = NR;
    while (stmt !~ /;[[:space:]]*$/ && (getline nextline) > 0) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", nextline);
      stmt = stmt " " nextline;
    }
    gsub(/[[:space:]]+/, " ", stmt);
    printf "%d\t%s%s\n", ln, attrs, stmt;
    flush_attrs(); next
  }
  # Anything else terminates a pending attribute run (it belonged to some other
  # item — a static, a use, an impl — which this section does not record).
  { flush_attrs() }
  function line_prefix() { return NR "\t" }
' "$LIB_RS" >"$DERIVED_DECLS"

DECL_COUNT="$(wc -l <"$DERIVED_DECLS" | tr -d ' ')"
[ "${DECL_COUNT:-0}" -gt 0 ] || fail "no crate-root \`pub mod\`/\`pub use\` declarations found in $LIB_RS_REL — the scan measured nothing. Refusing to pass."

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
while IFS=$'\t' read -r lineno decl; do
  case "$decl" in
    *"pub mod "*) ;;
    *) continue ;;
  esac
  case "$decl" in
    *"#[cfg("*|*"#[cfg_attr("*|*"doc(hidden)"*) continue ;;
  esac
  modname="${decl##*pub mod }"
  modname="${modname%;}"
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
done <"$DERIVED_DECLS"

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
#   * Granularity is item PATHS and KINDS, not SIGNATURES. This catches an added,
#     removed or renamed public item. It does NOT catch a changed parameter type, a
#     changed return type, a changed field, or a new/removed trait impl.
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
  echo "pub-surface: WROTE $SNAPSHOT_REL — $ITEM_COUNT public items over $MODULE_COUNT modules; $DECL_COUNT crate-root declarations."
  echo "             Review the diff: it is a public-API change, not a formatting chore."
  exit 0
fi

if [ ! -r "$SNAPSHOT" ]; then
  fail "committed snapshot $SNAPSHOT_REL is MISSING or unreadable. It is required — a missing snapshot is a FAIL, never an implicit pass. Create it with:
       bash scripts/ci/check-pub-surface.sh --regenerate"
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
echo "pub-surface: $ITEM_COUNT public items over $MODULE_COUNT modules match $SNAPSHOT_REL; $DECL_COUNT crate-root declarations consistent"
