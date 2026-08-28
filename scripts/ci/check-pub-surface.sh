#!/usr/bin/env bash
# check-pub-surface.sh — the CRATE-ROOT DECLARATION-CONSISTENCY GUARD for
# `cqlite-core` (issue #1712, epic #1688).
#
# # What this exists for
#
# `pub mod benchmarks;` sat unqualified at the crate root of `cqlite-core` for
# months, reading to every human and every reviewer as a shipped public module,
# while a `#![cfg(feature = "benchmarks")]` hidden INSIDE `benchmarks/mod.rs`
# quietly configured it out of every default build. The declaration site said one
# thing, the module's own file said another, and no mechanism could tell the
# difference.
#
# So this guard answers exactly ONE question, on every full gate:
#
#   Does the crate root TELL THE TRUTH about the modules it declares? An
#   unconditional, non-`#[doc(hidden)]` `pub mod NAME;` must not be gated by an
#   inner `#![cfg(...)]` inside NAME's own file.
#
# BOTH FACTS ARE IN THE SOURCE, and each is read from a BOUNDED input:
#
#   * the DECLARATION SITE — the crate-root scan below, which reads each top-level
#     declaration's attributes STRUCTURALLY (`attrs_verdict`) over one file,
#     `cqlite-core/src/lib.rs`;
#   * the MODULE'S OWN FILE — for each declaration the scan calls OPEN
#     (unconditional and not hidden), the module file's PROLOGUE is read and asked
#     one question: is there an inner `#![...]` attribute here that mentions `cfg`?
#
# THE PROLOGUE IS PROVABLY THE WHOLE INPUT, and this was MEASURED with rustc, not
# reasoned from the reference: an inner attribute is rejected both after an outer
# attribute ("an inner attribute is not permitted following an outer attribute")
# and after an item ("an inner attribute is not permitted in this context"). So
# everything before the first outer attribute or first item contains EVERY inner
# attribute the module has — the check does not need to read the rest of the file,
# and there is nowhere else for a module-level gate to hide.
#
# # WHAT THIS GUARD IS NOT — public-API drift detection (read this before filing a bug)
#
# It does NOT snapshot, diff or otherwise detect changes to `cqlite-core`'s public
# API. There is no `pub-surface.snapshot` and no `--regenerate`. That capability was
# built on top of a rustdoc-derived surface and REMOVED DELIBERATELY (issue #1712,
# lead ruling): five separate review findings were all one defect class — a lexical
# scanner that had to FIND DECLARATIONS ANYWHERE IN ARBITRARY SOURCE, an unbounded
# parsing problem where the code must reach a verdict on every line and therefore
# CANNOT ABSTAIN. What is left is bounded, single-question, and can refuse.
#
# The principled route to real drift detection is reachability from rustc's own
# dep-info rather than another text scan — issue #3366. Until that lands, nothing in
# this repo detects a public-API change, and a green here must not be read as one.
#
# # No invoker-selectable subject
#
# The target package and the crate root are HARD-CODED. No flag and no environment
# variable may select them, and none may be added. A gate component whose subject
# its invoker can choose can be pointed at a trivial subject and greened vacuously
# — the same reason `scripts/flow/roborev-review-oracles.sh` resolves its enforcer
# from its own directory with no override (CLAUDE.md: "the constrained party must
# not choose its own enforcer"). A test that needs a different subject SUBSTITUTES
# THE ARTIFACT in its own scratch checkout (`git worktree add --detach`), which is
# what scripts/tests/test_pub_surface_guard.sh does.
#
# # Fail-closed, affirmatively — REFUSE, NEVER GUESS
#
# A positive verdict requires an affirmative measurement (CLAUDE.md). There is no
# path through this script on which "nothing was measured" reads as PASS: a crate
# root it cannot fully read, a `pub mod` shape it does not recognise, zero
# declarations found, zero unconditional declarations checked, a module file that
# resolves to neither or both of its two legal paths, an unreadable module file, an
# inner attribute it cannot classify confidently, or a block comment in a prologue
# — each one is a NAMED FAIL that names the file. There is no opt-out.
#
# # Stated boundary (do not overclaim)
#
# GREEN means: every unconditional crate-root `pub mod NAME;` in
# cqlite-core/src/lib.rs was matched to exactly one module file whose prologue was
# read and carries no `cfg`-mentioning inner attribute. It means NOTHING about
# items inside those modules, about signatures, about re-exports, or about whether
# the public API changed. It does not look at any module that is NOT declared at
# the crate root, and it does not look past the prologue of the ones that are.
#
# Exit 0 = every unconditional crate-root declaration is consistent with its module
# file. 1 = an inconsistency, or a refusal. 2 = usage error.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Hard-coded subject — see "No invoker-selectable subject" above. Deliberately not
# parameterised, not env-overridable.
readonly LIB_RS_REL="cqlite-core/src/lib.rs"
readonly SRC_REL="cqlite-core/src"

LIB_RS="$REPO_ROOT/$LIB_RS_REL"

usage() {
  cat <<'EOF'
Usage: scripts/ci/check-pub-surface.sh [--help]

Crate-root declaration-consistency guard for the cqlite-core crate (issue #1712).

  (no flags)      Scan the top-level declarations of cqlite-core/src/lib.rs and, for
                  every unconditional non-#[doc(hidden)] `pub mod NAME;`, read
                  NAME's own module file and assert it does not gate itself with an
                  inner `#![cfg(...)]`. Refuses (exit 1) rather than guess on any
                  input it cannot classify.
  --help          This message.

This guard does NOT detect public-API drift: there is no snapshot and no
--regenerate mode (issue #1712 descope; the principled route is issue #3366).

The target package and crate root are hard-coded and cannot be selected by any flag
or environment variable, deliberately: a gate component must not let its invoker
point it at a trivial subject and pass vacuously.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
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

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pub-surface.XXXXXX")"
cleanup() { rm -rf "$WORK_DIR"; return 0; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1) Scan the CRATE-ROOT declarations of cqlite-core/src/lib.rs.
#
#    Deliberately FIRST: a crate root the guard cannot parse is a verdict-blocking
#    condition, so that failure is reported before any module file is opened.
#
#    Only the crate root, not the tree: this section records each top-level
#    declaration's SITE attributes (`#[cfg(...)]`, `#[doc(hidden)]`) so that step 2
#    knows WHICH declarations claim to be unconditional public exports, which is
#    exactly the set whose module files have to be read.
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
#    module escaped the consistency assert altogether.
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
# --- Structural reading of a declaration's ATTRIBUTES -------------------------
# Answers exactly one question — can this declaration be configured out, or hidden
# from the documented surface, under some configuration? — as GATED / HIDDEN / OPEN.
#
# It is STRUCTURAL, over meta-items, because substring matching on attribute text
# is the same defect class as the four parse shapes: `#[doc = "mentions
# doc(hidden)"]` and `#[cfg_attr(docsrs, doc(alias = "cfg(foo)"))]` both contain the
# tell-tale substrings while gating and hiding NOTHING, and under a substring test
# either one exempts a module from the consistency assert — a false PASS. String
# literal CONTENTS are therefore erased before anything is parsed: an attribute
# VALUE is data, never structure.
function strip_strings(t,   out, i, c, instr) {
  out = ""; instr = 0
  for (i = 1; i <= length(t); i++) {
    c = substr(t, i, 1)
    if (instr) {
      if (c == "\\") { i++; continue }
      if (c == "\"") { instr = 0; out = out "\"" }
      continue
    }
    if (c == "\"") { instr = 1; out = out "\""; continue }
    out = out c
  }
  return out
}
# Split the inside of a meta-list into its TOP-LEVEL comma-separated items.
function split_meta(str, arr,   i, d, c, cur, k) {
  k = 0; cur = ""; d = 0
  for (i = 1; i <= length(str); i++) {
    c = substr(str, i, 1)
    if (c == "(") d++
    else if (c == ")") d--
    if (c == "," && d == 0) { if (ltrim(rtrim(cur)) != "") { k++; arr[k] = ltrim(rtrim(cur)) } ; cur = ""; continue }
    cur = cur c
  }
  if (ltrim(rtrim(cur)) != "") { k++; arr[k] = ltrim(rtrim(cur)) }
  return k
}
function meta_verdict(m,   nm, rest, args, parts, k, i, v, r) {
  m = ltrim(rtrim(m))
  if (!match(m, /^[A-Za-z_][A-Za-z0-9_]*/)) return "OPEN"
  nm = substr(m, RSTART, RLENGTH)
  rest = ltrim(substr(m, RLENGTH + 1))
  args = ""
  if (substr(rest, 1, 1) == "(" && substr(rest, length(rest), 1) == ")")
    args = substr(rest, 2, length(rest) - 2)
  if (nm == "cfg") return "GATED"
  if (nm == "doc") {
    if (args == "") return "OPEN"
    k = split_meta(args, parts)
    for (i = 1; i <= k; i++) if (parts[i] == "hidden") return "HIDDEN"
    return "OPEN"
  }
  if (nm == "cfg_attr") {
    k = split_meta(args, parts)
    r = "OPEN"
    # parts[1] is the PREDICATE; parts[2..] are the attributes it would apply.
    for (i = 2; i <= k; i++) {
      v = meta_verdict(parts[i])
      if (v == "GATED") return "GATED"
      if (v == "HIDDEN") r = "HIDDEN"
    }
    return r
  }
  return "OPEN"
}
function attrs_verdict(a,   i, j, c, d, m, v, res) {
  a = strip_strings(a)
  res = "OPEN"
  i = 1
  while (i <= length(a)) {
    if (substr(a, i, 2) != "#[") { i++; continue }
    d = 0; j = i + 1
    while (j <= length(a)) {
      c = substr(a, j, 1)
      if (c == "[") d++
      else if (c == "]") { d--; if (d == 0) break }
      j++
    }
    if (j > length(a)) return "GATED"   # unbalanced: cannot read it, so do not exempt on OPEN
    v = meta_verdict(substr(a, i + 2, j - i - 2))
    if (v == "GATED") return "GATED"
    if (v == "HIDDEN") res = "HIDDEN"
    i = j + 1
  }
  return res
}
function rtrim(x) { sub(/[[:space:]]+$/, "", x); return x }
function squash(x) { gsub(/[[:space:]]+/, " ", x); return ltrim(rtrim(x)) }

# Blank out comments, preserving line structure and column positions, and record
# for each line whether it STARTS in ordinary code (a line that begins inside a
# string or a block comment can never open a crate-root declaration).
#
# It also records BRACE_MIN[i]: the LOWEST brace nesting depth the line reaches,
# counting only braces in ordinary code (never inside a comment, a string, a raw
# string or a char literal). Depth zero is what makes a declaration a CRATE-ROOT
# declaration — leading whitespace does not, and Rust permits an indented top-level
# item. Refusal I below is the only consumer; the two derivations keep their pinned
# column-zero rule (see the comment there for why).
function normalize(   i, s, out, j, c, c2, st, depth, hashes, k, cnt, bd, bmin) {
  st = "code"; depth = 0; hashes = 0; bd = 0
  for (i = 1; i <= n; i++) {
    INCODE[i] = (st == "code") ? 1 : 0
    bmin = bd
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
        # A CHAR LITERAL can carry a brace (`'{'`), which would desync the counter
        # below. This recognises only the two simple shapes `'x'` and `'\x'` and does
        # NOT try to be a Rust lexer: a lifetime (`'a`) has no closing quote so it
        # never matches, and `'\u{7f}'` falls through with its braces BALANCED, which
        # costs the counter nothing. The text copied to `out` is unchanged, so the
        # scan the two derivations read is byte-identical to before.
        if (c == "'" && match(substr(s, j), /^'(\\.|[^'])'/)) {
          out = out substr(s, j, RLENGTH); j += RLENGTH; continue
        }
        if (c == "{") bd++
        else if (c == "}") {
          bd--
          if (bd < bmin) bmin = bd
          # Depth below zero means the count is WRONG (a shape this is not a parser
          # for), and a count that is wrong HIGH under-fires Refusal I — the silent
          # direction. So an unreliable count makes every indented `pub mod` refuse.
          if (bd < 0) BRACE_UNRELIABLE = 1
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
    BRACE_MIN[i] = bmin
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

  # --- Refusal X: a line that does NOT START in ordinary code yet carries CODE.
  #
  # `INCODE[i]` records only the comment/string state at the START of line i, so a
  # declaration that follows a closing delimiter ON THE SAME LINE — `*/ pub mod x;`
  # — is skipped by BOTH derivations (each one bails on `!INCODE`). They therefore
  # AGREE while both are blind, exactly like Refusals U and I, and the mutual
  # cross-check cannot see a blind spot the two derivations SHARE (issue #1712,
  # roborev r7 finding 2). An inner-gated module declared that way would pass GREEN.
  #
  # WHY A REFUSAL AND NOT A MODEL. Tracking mid-line comment state so those
  # declarations could be COLLECTED means a block-comment state machine underneath
  # the primary collection rule of both derivations — a second implementation of
  # Rust's lexer, defeatable in the false-PASS direction, which is the defect class
  # this whole guard has already paid for five times. A refusal is bounded, obviously
  # correct and cannot rot; the cost is one loud FAIL with a one-word remedy (put the
  # code on its own line).
  #
  # DELIBERATELY OVER-APPROXIMATE, in the safe direction: it fires on ANY non-blank
  # code left on such a line, `pub mod` or not, and on a line that resumes after a
  # multi-line string literal too. Over-firing costs a named FAIL; under-firing costs
  # a silent false PASS.
  for (i = 1; i <= n; i++) {
    if (INCODE[i]) continue
    if (N[i] == "") continue
    printf "X\tline %d: code follows a closing block-comment/string delimiter on the SAME line: `%s`\n", i, squash(substr(ltrim(N[i]), 1, 72))
  }

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

  # --- Refusal U: a top-level `pub mod` occurrence NEITHER derivation consumed.
  #
  # Both derivations above recognise exactly ONE shape: the statement form
  # `pub mod NAME;`. An INLINE module — `pub mod NAME { #![cfg(...)] ... }` — is
  # invisible to BOTH, so they AGREE (each derived the empty set for it) while both
  # are blind. The mutual cross-check cannot catch a blind spot the two derivations
  # SHARE, and a `#![cfg]` inside that body is the exact bypass this assert exists to
  # close, so the scan REFUSES over a crate root declaring a public module in a shape
  # it does not handle. It deliberately does NOT parse the body: that road is a Rust
  # parser, and this guard has already paid for four substring-vs-structure defects.
  #
  # DELIBERATELY OVER-APPROXIMATE, in the safe direction — it also fires on a
  # multi-line `pub mod` / `NAME;` split across lines, on `pub  mod NAME;` (two
  # spaces), and on the token inside a non-indented string literal, none of which
  # either derivation recognises either. Over-firing costs a loud, actionable FAIL;
  # under-firing costs a silent false PASS, which is the defect being fixed here.
  #
  # `pub(crate) mod` / `pub(super) mod` / `pub(in path) mod` are OUT OF SCOPE and are
  # deliberately not matched: this assert's subject is the PUBLIC surface, and a
  # restricted-visibility module is not reachable from outside the crate, so it can
  # hide a gate that changes the crate's public surface.
  # (cqlite-core's crate root carries exactly such an inline module today,
  # `pub(crate) mod test_alloc_probe`, which must stay green.)
  for (i = 1; i <= n; i++) {
    if (!INCODE[i]) continue
    t = N[i]
    if (t ~ /^[[:space:]]/ || t == "") continue
    rest = t
    while (match(rest, /pub[[:space:]]+mod([^A-Za-z0-9_]|$)/)) {
      # An identifier character immediately before the match means this is not the
      # `pub` keyword at all (`repub mod`), so it declares nothing.
      pre = (RSTART > 1) ? substr(rest, RSTART - 1, 1) : ""
      tail = substr(rest, RSTART)
      if (pre !~ /[A-Za-z0-9_]/ && tail !~ /^pub mod [A-Za-z_][A-Za-z0-9_]*[[:space:]]*;/) {
        # squash() also collapses any TAB in the snippet, which keeps the record's
        # single text field from splitting under the tab-field convention below.
        printf "U\tline %d: unrecognized top-level `pub mod` form: `%s`\n", i, squash(substr(tail, 1, 72))
      }
      rest = substr(rest, RSTART + RLENGTH)
    }
  }

  # --- Refusal I: an INDENTED `pub mod` at BRACE DEPTH ZERO.
  #
  # Rust does not require a top-level item to start at column zero, but every scan
  # path above keys on column zero. So an indented `pub mod x;` whose module file
  # carries an inner `#![cfg(...)]` is absent from S, absent from P AND absent from
  # from step 2's examined set: the two derivations AGREE (both derived nothing for
  # it) while both are blind, the cross-check is satisfied, and the crate-root
  # inconsistency this assert exists to catch passes GREEN. Same shape as Refusal U — a SHARED blind spot is
  # not a disagreement — so it gets the same treatment: refuse, do not guess.
  #
  # WHY A REFUSAL AND NOT "COLLECT INDENTED DEPTH-0 DECLARATIONS TOO" (the choice, and
  # its cost). Teaching S and P to collect by depth instead of by column is the more
  # CORRECT reading, and it was rejected deliberately: it puts a hand-written brace
  # counter underneath the primary collection rule of BOTH derivations, so a counter
  # bug (this is a lexical scan, not a Rust parser — `'{'`, macro token trees, shapes
  # nobody has thought of) becomes a blind spot the two derivations SHARE, which is
  # exactly the defect class being fixed here and exactly what their mutual
  # cross-check cannot see. Keeping S at its pinned "simplest rule that can be
  # written" — column-zero, no depth, no attributes — is what makes the cross-check
  # worth anything. So the depth counter is confined to THIS refusal, where its only
  # authority is to decide whether to fire, and every branch of it is set to fire.
  #
  # DELIBERATELY OVER-APPROXIMATE, in the safe direction, in three ways: it fires on
  # any depth-0 indented `pub mod` shape (statement, inline, split across lines); it
  # uses the line's MINIMUM depth, so `    } pub mod x;` (genuinely crate-root) fires,
  # at the price of also firing on the exotic `    pub mod inner; }` inside a block;
  # and an unreliable count fires everything. Over-firing costs a loud FAIL with a
  # one-word remedy (dedent it); under-firing costs a silent false PASS.
  #
  # It must NOT fire below depth 0: `mod outer { pub mod inner; }` is ordinary Rust,
  # `inner` is not a crate-root declaration, and a refusal that reds correct code is
  # a refusal agents learn to waive.
  for (i = 1; i <= n; i++) {
    if (!INCODE[i]) continue
    t = N[i]
    if (t == "" || t !~ /^[[:space:]]/) continue
    if (!BRACE_UNRELIABLE && BRACE_MIN[i] != 0) continue
    rest = t
    while (match(rest, /pub[[:space:]]+mod([^A-Za-z0-9_]|$)/)) {
      pre = (RSTART > 1) ? substr(rest, RSTART - 1, 1) : ""
      tail = substr(rest, RSTART)
      if (pre !~ /[A-Za-z0-9_]/) {
        printf "I\tline %d: INDENTED crate-root (brace depth 0) `pub mod`: `%s`\n", i, squash(substr(tail, 1, 72))
      }
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
        # Rust permits blank lines and comments (including `///` doc comments, which
        # normalize to empty here) BETWEEN an attribute and the item it applies to,
        # so those must not end the attribute run: breaking on them made a genuinely
        # gated module read as unconditional and reported it INCONSISTENT.
        nxt = CUR + 1
        while (nxt <= n && INCODE[nxt] && N[nxt] == "") nxt++
        if (nxt > n) break
        if (!INCODE[nxt]) break
        if (N[nxt] ~ /^[[:space:]]/) break
        CUR = nxt
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
      # Fields: line, name, attribute VERDICT. The rendered attribute text lives on
      # the `D` record only — a possibly-EMPTY field must never sit before a
      # meaningful one here, because bash `read` with IFS=<tab> collapses runs of
      # tabs (tab is IFS whitespace), which would silently shift every later field.
      printf "M\t%d\t%s\t%s\n", CUR, nm, attrs_verdict(attrs)
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

# CODE AFTER A CLOSING DELIMITER on a line the scan reads as comment/string state.
# Another blind spot the two derivations SHARE — both skip a line whose START is not
# ordinary code — so it needs its own channel rather than the cross-check, which by
# construction cannot see it (issue #1712, roborev r7 F2).
if grep -q '^X	' "$SCAN_RAW"; then
  echo "" >&2
  grep '^X	' "$SCAN_RAW" | cut -f2- >&2
  fail "the crate root $LIB_RS_REL carries CODE after a closing block-comment or string delimiter on the SAME line (see above). The scan records comment/string state only at the START of each line, so both of its derivations skip such a line entirely: they AGREE while both are blind, and a \`*/ pub mod NAME;\` whose module file hides an inner \`#![cfg(...)]\` would sail through the consistency assert this guard exists to be.
       The guard REFUSES rather than model mid-line comment state — that is a second implementation of Rust's lexer sitting underneath the primary collection rule of both derivations, and every one of the five defects already fixed on this guard was a scanner of exactly that shape.
       Remedy: put the code on its own line, at column zero."
fi

# A `pub mod` shape NEITHER derivation consumed. This is NOT a disagreement between
# the two derivations — it is a blind spot they SHARE, which is precisely why the
# cross-check further down cannot catch it and why this refusal is its own channel.
if grep -q '^U	' "$SCAN_RAW"; then
  echo "" >&2
  grep '^U	' "$SCAN_RAW" | cut -f2- >&2
  fail "the crate root $LIB_RS_REL declares a public module in a form the crate-root scan does not handle (see above). The scan handles exactly one shape: the statement form \`pub mod NAME;\`.
       An INLINE module (\`pub mod NAME { ... }\`) can carry its own \`#![cfg(...)]\` INNER attribute — the gate hides inside the body while the crate root advertises the module unconditionally, which is the exact bypass this consistency assert exists to close. Both derivations are blind to the inline form, so they AGREE and the cross-check below cannot see it; the guard refuses rather than report a verdict it cannot support.
       Remedy: declare it as a FILE module — \`pub mod NAME;\` in $LIB_RS_REL plus NAME.rs (or NAME/mod.rs) — and put any cfg gate at the DECLARATION SITE (\`#[cfg(feature = \"...\")] pub mod NAME;\`), where this assert reads it structurally."
fi

# An INDENTED `pub mod` at brace depth zero. Also a blind spot the two derivations
# SHARE (both key on column zero), so it too needs its own channel rather than the
# cross-check, which by construction cannot see it.
if grep -q '^I	' "$SCAN_RAW"; then
  echo "" >&2
  grep '^I	' "$SCAN_RAW" | cut -f2- >&2
  fail "the crate root $LIB_RS_REL declares a public module on an INDENTED line at brace depth zero (see above). Rust accepts that as a top-level declaration; this scan does not read it — both of its derivations key on column zero, so they AGREE while both are blind, and a module whose file carries an inner \`#![cfg(...)]\` would then be advertised unconditionally by the crate root while being absent from the compiled crate, and never examined by step 2. That is the exact bypass this consistency assert exists to close, so the guard refuses rather than report a verdict it cannot support.
       (An indented \`pub mod\` NESTED inside \`mod outer { ... }\` is ordinary Rust and is NOT reported: what makes a declaration crate-root is brace depth zero, not column zero.)
       Remedy: dedent it to column zero — \`pub mod NAME;\` — and put any cfg gate at the DECLARATION SITE (\`#[cfg(feature = \"...\")] pub mod NAME;\`), where this assert reads it structurally."
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

# ---------------------------------------------------------------------------
# 2) THE MODULE-FILE ORACLE — answer the other half of the question FROM SOURCE.
#
#    THE DEFECT, stated exactly (issue #1712): the DECLARATION SITE says
#    unconditional while the MODULE'S OWN FILE says gated. Step 1 read the
#    declaration site. This step reads the module file. Nothing else is needed, and
#    nothing else is consulted.
#
#    WHY THIS IS NOT THE SCANNER CLASS THAT WAS DELETED HERE (read before "improving"
#    it). Five review findings on this issue were all one shape: a hand-rolled scan
#    that had to FIND DECLARATIONS ANYWHERE IN ARBITRARY SOURCE — an unbounded
#    parsing problem where the code must reach a verdict on every line and therefore
#    CANNOT ABSTAIN. This question is the opposite in all three respects that matter:
#
#      * BOUNDED INPUT — ONE named file, and only its PROLOGUE.
#      * ONE QUESTION — "is there an inner attribute here that mentions `cfg`?"
#      * IT CAN REFUSE — and on anything it cannot classify confidently, it does.
#
#    THE PROLOGUE IS PROVABLY THE WHOLE INPUT, MEASURED WITH rustc rather than read
#    out of the reference:
#        #[allow(dead_code)]  +  #![allow(unused)]  =>
#            error: an inner attribute is not permitted following an outer attribute
#        pub fn f() {}        +  #![allow(unused)]  =>
#            error: an inner attribute is not permitted in this context
#    So every inner attribute a module has sits before its first outer attribute and
#    before its first item. Stopping at whichever comes first cannot miss one: there
#    is nowhere else in the file for a module-level gate to hide.
#
#    THE FOUR THINGS THE PROLOGUE READER DOES, and nothing more:
#      1. skip blank lines;
#      2. skip a line whose first non-space characters are `//` — a line comment is
#         UNAMBIGUOUSLY terminated by the newline, so skipping it needs no state and
#         cannot desynchronise (this is the one comment form that is safe to handle);
#      3. REFUSE on `/*` — see below;
#      4. on `#![`, consume the attribute by BRACKET BALANCE (appending lines until it
#         closes) and classify it; on anything else, the prologue is OVER — stop.
#
#    WHY `/*` IS A REFUSAL AND NOT A FEATURE. `/* #![cfg(feature = "x")] */` before
#    the first item is a delimiter inside a comment, i.e. the SAME class as the five
#    deleted findings, and handling it means a block-comment state machine — nesting,
#    `/*` inside a string, `*/` inside a string. The lead's ruling on this guard is
#    explicit: prefer the refusal, because "it is bounded, obviously correct, and
#    cannot rot". So a block comment opening in a prologue is a NAMED FAIL with a
#    one-line remedy (use `//`), never a modelled state.
#
#    THE CLASSIFICATION, and why it splits into TWO non-passing verdicts. An inner
#    attribute whose leading identifier is `cfg` can configure the module out — that
#    is THE DEFECT, and it gets the defect diagnostic with the hoist remedy. Anything
#    else that so much as MENTIONS a `cfg`-prefixed token (`cfg_attr`, which can apply
#    a `cfg`; a `cfg` token anywhere in the attribute, including inside a string) is
#    REFUSED, not exempted: deciding that such an attribute gates nothing means
#    parsing meta-items and erasing string contents, which is the parser this guard
#    has already paid for five times. Both verdicts exit non-zero, so no
#    classification gap can become a false PASS; they carry DIFFERENT text so the
#    operator knows which one they hit. An attribute mentioning no `cfg` token at all
#    (`#![allow(...)]`, `#![doc = "..."]`, `#![no_std]`) is inert for this question
#    and the scan moves on.
#
#    OVER-APPROXIMATION IS DELIBERATE AND ONE-DIRECTIONAL, as everywhere else in this
#    guard: a cosmetic `#![cfg_attr(docsrs, doc(...))]` in a prologue REFUSES and
#    costs one loud FAIL naming the file and line. Under-firing costs a silent false
#    PASS in the one assert this guard is. Those costs are not comparable.
# ---------------------------------------------------------------------------
PROLOGUE_AWK="$WORK_DIR/prologue.awk"
cat >"$PROLOGUE_AWK" <<'PROLOGUE_AWK_EOF'
# Read ONE module file's PROLOGUE and emit exactly one tab-separated record:
#
#   CLEAN  <line>                     prologue ended at <line>; no cfg-mentioning
#                                     inner attribute in it
#   GATED  <line>  <attr>             an inner `#![cfg(...)]` — THE #1712 defect
#   REFUSE <line>  <why>              cannot classify; the guard FAILs naming it
#
# NOT a Rust parser and it must not become one — see the guard's "2)" comment block.
function ltrim(x) { sub(/^[[:space:]]+/, "", x); return x }
function rtrim(x) { sub(/[[:space:]]+$/, "", x); return x }
function squash(x) { gsub(/[[:space:]]+/, " ", x); return ltrim(rtrim(x)) }
function refuse(ln, why) { printf "REFUSE\t%d\t%s\n", ln, why; DONE = 1 }
# Does the attribute text mention any `cfg`-prefixed IDENTIFIER TOKEN? Token-bounded
# so `config`/`configure` do not match, and deliberately NOT string-aware: a `cfg`
# inside a string literal here costs a REFUSAL (safe), while erasing string contents
# would be one more sub-scanner to get wrong in the false-PASS direction.
function mentions_cfg(t,   i, n, c, j, tok) {
  n = length(t)
  for (i = 1; i <= n; i++) {
    c = substr(t, i, 1)
    if (c !~ /[A-Za-z0-9_]/) continue
    j = i
    while (j <= n && substr(t, j, 1) ~ /[A-Za-z0-9_]/) j++
    tok = substr(t, i, j - i)
    i = j - 1
    if (tok ~ /^cfg/) return 1
  }
  return 0
}
# The attribute's leading identifier: `#![NAME...`. "" if there is not one.
function attr_name(t,   rest) {
  rest = ltrim(substr(t, 4))          # drop `#![`
  if (match(rest, /^[A-Za-z_][A-Za-z0-9_]*/)) return substr(rest, RSTART, RLENGTH)
  return ""
}
BEGIN { BOM = sprintf("%c%c%c", 239, 187, 191) }
# rustc ACCEPTS AND IGNORES one leading UTF-8 BOM, so a BOM-prefixed file whose first
# line is `#![cfg(...)]` IS gated by the compiler. Without stripping it the `#![` test
# below fails, the prologue reads CLEAN, and the gate is HIDDEN — a false PASS
# (roborev r8 F2). Compared as BYTES, not via a \x regex, which is not portable.
{ if (NR == 1 && substr($0, 1, 3) == BOM) $0 = substr($0, 4); L[NR] = $0 }
END {
  n = NR
  i = 1
  while (i <= n) {
    t = ltrim(L[i])
    if (t == "") { i++; continue }
    # A `//` line comment is unambiguously terminated by the newline, so skipping the
    # line needs no state at all. This is the ONLY comment form handled; `/*` refuses.
    if (substr(t, 1, 2) == "//") { i++; continue }
    if (substr(t, 1, 2) == "/*") {
      refuse(i, "a BLOCK COMMENT opens in the module prologue: `" squash(substr(t, 1, 72)) "`")
      exit
    }
    if (substr(t, 1, 3) != "#![") {
      # First outer attribute or first item: rustc forbids an inner attribute after
      # either, so the prologue — and every inner attribute in the file — ends here.
      printf "CLEAN\t%d\n", i
      exit
    }
    # A `cfg`-NAMED inner attribute IS the #1712 defect, and it is decided HERE, from
    # the NAME, BEFORE any bracket balancing. Two reasons, both load-bearing:
    #   * the defect's canonical spelling `#![cfg(feature = "x")]` CONTAINS STRING
    #     QUOTES and may span lines, so deciding it after the balancing scan would let
    #     one of that scan's refusals downgrade the precise named diagnostic — which
    #     carries the #1712 hoist remedy — to a generic "cannot classify";
    #   * a NAME is readable without counting anything, so no bracket-counting defect
    #     can ever hide the ONE shape this guard exists to catch.
    if (attr_name(t) == "cfg") {
      printf "GATED\t%d\t%s\n", i, squash(t)
      exit
    }
    # An inner attribute. Consume it by BRACKET BALANCE, appending lines as needed.
    buf = t
    startline = i
    cur = i
    d = 0
    p = 3                              # position of the `[` in `#![`
    endpos = 0
    while (1) {
      if (p > length(buf)) {
        cur++
        if (cur > n) { refuse(startline, "an inner attribute starting here never closes its `[`"); exit }
        buf = buf " " ltrim(L[cur])
        continue
      }
      ch = substr(buf, p, 1)
      nx = substr(buf, p + 1, 1)
      # A COMMENT inside an attribute window cannot be terminated by this scan: lines
      # are JOINED WITH A SPACE above, so a `//`'s newline terminator is already gone.
      # roborev r8 F1: `#![allow(dead_code, // ]` ends its line with a `]` INSIDE a
      # comment, so bracket counting closes the window EARLY; `rest` is empty (nothing
      # follows on the line, so the same-line check cannot fire) and the scan resumes
      # MID-ATTRIBUTE, missing a LATER `#![cfg(...)]` entirely. A false PASS.
      if (ch == "/" && (nx == "/" || nx == "*")) {
        refuse(startline, "an inner attribute contains a COMMENT, whose end this bounded scan cannot locate, so its brackets cannot be counted: `" squash(substr(buf, 1, 72)) "`")
        exit
      }
      # A RAW STRING delimiter is `r` + N hashes + `"`. Modelling N is a second lexer;
      # refusing is bounded and obviously correct.
      if (ch == "r" && nx == "#") {
        refuse(startline, "an inner attribute contains a RAW STRING, whose delimiter length this bounded scan does not model: `" squash(substr(buf, 1, 72)) "`")
        exit
      }
      # A bare `\047` is a char literal OR a lifetime, and telling them apart needs
      # context. (One INSIDE a string never reaches here; the string is skipped below.)
      if (ch == "\047") {
        refuse(startline, "an inner attribute contains a char-literal-or-lifetime quote this bounded scan does not model: `" squash(substr(buf, 1, 72)) "`")
        exit
      }
      # A NORMAL string is SKIPPED, not refused: `#![doc = "..."]` is a real idiom that
      # case 36 pins. A guard that refused it would red CORRECT code, and a refusal
      # that reds correct code is the one agents learn to waive — which would make
      # every other refusal in this scan worthless.
      if (ch == "\"") {
        p++
        while (1) {
          if (p > length(buf)) {
            cur++
            if (cur > n) { refuse(startline, "an inner attribute contains a STRING that never closes: `" squash(substr(buf, 1, 72)) "`"); exit }
            buf = buf " " ltrim(L[cur])
            continue
          }
          sc = substr(buf, p, 1)
          if (sc == "\\") { p += 2; continue }
          if (sc == "\"") { p++; break }
          p++
        }
        continue
      }
      if (ch == "[") d++
      else if (ch == "]") { d--; if (d == 0) { endpos = p; break } }
      p++
    }
    attr = substr(buf, 1, endpos)
    rest = ltrim(rtrim(substr(buf, endpos + 1)))
    # ANYTHING after the closing `]` is refused rather than parsed. A second inner
    # attribute on the same line (`#![doc = "]"] #![cfg(x)]`) would otherwise be
    # invisible — a false PASS — and a trailing `/*` would desynchronise the lines
    # after it. Rustfmt never emits either shape.
    if (rest != "") {
      refuse(startline, "content follows an inner attribute on the SAME line: `" squash(substr(rest, 1, 60)) "`")
      exit
    }
    if (index(attr, "/*") > 0) {
      refuse(startline, "an inner attribute contains a block comment: `" squash(attr) "`")
      exit
    }
    nm = attr_name(attr)
    if (nm == "") {
      refuse(startline, "an inner attribute whose name cannot be read: `" squash(substr(attr, 1, 72)) "`")
      exit
    }
    if (nm == "cfg") {
      printf "GATED\t%d\t%s\n", startline, squash(attr)
      exit
    }
    if (mentions_cfg(attr)) {
      refuse(startline, "an inner attribute mentions a `cfg` token and cannot be confidently classified: `" squash(substr(attr, 1, 72)) "`")
      exit
    }
    i = cur + 1
  }
  # Ran off the end: the whole file is prologue (blank lines, `//` comments and inert
  # inner attributes) and it carries no cfg-mentioning inner attribute.
  printf "CLEAN\t%d\n", n + 1
}
PROLOGUE_AWK_EOF

# ---------------------------------------------------------------------------
# 3) THE CONSISTENCY ASSERT (the core of #1712).
#
#    Every crate-root `pub mod NAME;` that carries NO declaration-site `#[cfg(...)]`
#    and is NOT `#[doc(hidden)]` reads to every human as an unconditional public
#    export. Its module file must therefore not gate itself: if it does, the gate is
#    hiding inside the module file where no reader of the crate root can see it —
#    exactly the #1712 defect.
# ---------------------------------------------------------------------------
OPEN_COUNT=0
READ_COUNT=0
inconsistent=0
PROLOGUE_OUT="$WORK_DIR/prologue.txt"

while IFS=$'\t' read -r lineno modname gate; do
  # Which declarations are EXEMPT from the assert, and why each one is:
  #
  #   GATED  — a real `#[cfg(...)]` at the declaration site, or a `cfg_attr` that
  #            could itself APPLY a `cfg`. The gate is visible to every reader of
  #            the crate root, which is exactly what this assert wants; and under
  #            some configuration the module legitimately is not in the surface.
  #   HIDDEN — a real `doc(hidden)` meta-item, directly or as a `cfg_attr` output.
  #            The declaration is deliberately not part of the documented surface,
  #            so it makes no unconditional promise to a reader.
  #   OPEN   — neither, so the declaration reads as an unconditional public export
  #            and its module file MUST NOT gate it.
  #
  # The verdict is computed STRUCTURALLY over meta-items by `attrs_verdict` in the
  # crate-root scan, with string-literal contents erased first (an attribute VALUE
  # is data, never structure). A substring test here would be the same defect class
  # as the parse shapes above: `#[doc = "mentions doc(hidden)"]` and
  # `#[cfg_attr(docsrs, doc(alias = "cfg(foo)"))]` gate and hide NOTHING, yet each
  # contains the tell-tale substring, and either would exempt an inner-gated module
  # from this assert — a false PASS.
  case "$gate" in
    GATED|HIDDEN) continue ;;
    OPEN) ;;
    *) fail "the crate-root scan produced an unrecognised attribute verdict '$gate' for \`pub mod $modname\` at $LIB_RS_REL:$lineno. An unplanned verdict is not an exemption; refusing to pass." ;;
  esac
  OPEN_COUNT=$((OPEN_COUNT + 1))

  # RESOLVE THE MODULE FILE. Rust gives a non-inline module exactly two legal paths,
  # and rustc itself rejects both existing at once ("file for module found at both").
  # Neither, both, or a path that is not a readable regular file is a REFUSAL naming
  # what was looked for — never a skip. A skip here is a silent false PASS: the
  # declaration would go unexamined while the run still reports success.
  mod_file_rel="$SRC_REL/$modname.rs"
  mod_dir_rel="$SRC_REL/$modname/mod.rs"
  mod_file="$REPO_ROOT/$mod_file_rel"
  mod_dir="$REPO_ROOT/$mod_dir_rel"
  found=0
  resolved=""
  resolved_rel=""
  if [ -e "$mod_file" ]; then found=$((found + 1)); resolved="$mod_file"; resolved_rel="$mod_file_rel"; fi
  if [ -e "$mod_dir" ]; then found=$((found + 1)); resolved="$mod_dir"; resolved_rel="$mod_dir_rel"; fi
  if [ "$found" -eq 0 ]; then
    fail "\`pub mod $modname\` at $LIB_RS_REL:$lineno reads as an unconditional public export, but NEITHER of its two legal module files exists:
           $mod_file_rel
           $mod_dir_rel
       The guard cannot read the module's own prologue, so it cannot tell whether the module gates itself — and it refuses to pass a declaration it did not examine.
       Causes: the declaration names a module that is not there (the crate does not compile), or the declaration carries a \`#[path = \"...\"]\` attribute relocating its source. This guard deliberately does not resolve \`#[path\`: put any cfg gate at the DECLARATION SITE instead, where step 1 reads it structurally."
  fi
  if [ "$found" -gt 1 ]; then
    fail "\`pub mod $modname\` at $LIB_RS_REL:$lineno resolves to BOTH of its legal module files:
           $mod_file_rel
           $mod_dir_rel
       rustc rejects that too (\"file for module found at both\"), and the guard will not choose one of them — refusing rather than guess which file is the module."
  fi
  if [ ! -f "$resolved" ] || [ ! -r "$resolved" ]; then
    fail "the module file $resolved_rel for \`pub mod $modname\` ($LIB_RS_REL:$lineno) exists but is not a READABLE REGULAR FILE (a directory, a dangling symlink, or unreadable permissions). Refusing to report a verdict over a module file it could not read."
  fi

  # LC_ALL=C so the reader is BYTE-oriented. The BOM test compares three raw bytes,
  # and under a UTF-8 locale `sprintf("%c", 239)` yields the CHARACTER U+00EF (two
  # bytes) rather than the byte 0xEF, so the test silently never matches and the BOM
  # false PASS (roborev r8 F2) comes straight back. Verified in both locales.
  if ! LC_ALL=C awk -f "$PROLOGUE_AWK" "$resolved" >"$PROLOGUE_OUT" 2>"$WORK_DIR/prologue.err"; then
    echo "" >&2
    cat "$WORK_DIR/prologue.err" >&2
    fail "the prologue reader errored on $resolved_rel (see above). Refusing to report a verdict over a module file it could not read."
  fi
  [ "$(wc -l <"$PROLOGUE_OUT" | tr -d ' ')" -eq 1 ] \
    || fail "the prologue reader produced $(wc -l <"$PROLOGUE_OUT" | tr -d ' ') records for $resolved_rel, expected exactly 1. Refusing to pass on an unreadable result."

  IFS=$'\t' read -r verdict at detail <"$PROLOGUE_OUT" || true
  case "$verdict" in
    CLEAN)
      READ_COUNT=$((READ_COUNT + 1))
      ;;
    REFUSE)
      fail "the prologue of $resolved_rel could not be read confidently, at $resolved_rel:$at — $detail
       That module is declared \`pub mod $modname;\` at $LIB_RS_REL:$lineno with no declaration-site gate, so this guard has to establish that the module file does not gate itself; it cannot, so it REFUSES rather than guess.
       Remedy: use \`//\` line comments in the prologue, keep one inner attribute per line, and put any cfg gate at the DECLARATION SITE (\`#[cfg(feature = \"...\")] pub mod $modname;\`) where step 1 reads it structurally."
      ;;
    GATED)
      READ_COUNT=$((READ_COUNT + 1))
      inconsistent=$((inconsistent + 1))
      echo "" >&2
      echo "❌ pub-surface: crate-root declaration is INCONSISTENT with the module's own file" >&2
      echo "" >&2
      echo "    \`pub mod $modname\` at $LIB_RS_REL:$lineno reads as an unconditional public" >&2
      echo "    export, but its module file GATES ITSELF:" >&2
      echo "" >&2
      echo "        $resolved_rel:$at:  $detail" >&2
      echo "" >&2
      echo "    The cfg gate lives INSIDE the module file, where no reader of the crate root" >&2
      echo "    can see it: the crate root says the module ships, the compiled crate does not" >&2
      echo "    have it." >&2
      echo "" >&2
      echo "    Remedy — hoist the gate to the declaration site so the crate root tells the truth:" >&2
      echo "        #[cfg(feature = \"…\")]" >&2
      echo "        pub mod $modname;" >&2
      echo "    …and delete the inner attribute from $resolved_rel. Or, if it is deliberately" >&2
      echo "    an undocumented internal surface, mark the declaration" >&2
      echo "        #[doc(hidden)]" >&2
      echo "        pub mod $modname;" >&2
      ;;
    *)
      fail "the prologue reader returned an unrecognised verdict '$verdict' for $resolved_rel. An unplanned verdict is not an exemption; refusing to pass."
      ;;
  esac
done <"$DERIVED_MODS"

[ "$inconsistent" -eq 0 ] \
  || fail "$inconsistent crate-root declaration(s) are advertised unconditionally while gating themselves inside their own module file (see above). Issue #1712."

# AFFIRMATIVE MEASUREMENT, not "no error observed" (CLAUDE.md). Zero unconditional
# declarations, or a prologue count that does not account for every one of them,
# means the assert examined nothing (or not everything) — which is the vacuous pass
# itself, so each is a NAMED FAIL rather than a quiet success.
[ "$OPEN_COUNT" -gt 0 ] \
  || fail "the crate root $LIB_RS_REL declares $MOD_COUNT module(s) but NOT ONE of them is an unconditional, non-\`#[doc(hidden)]\` \`pub mod\`, so this assert examined nothing. A positive verdict requires an affirmative measurement; refusing to pass."
[ "$READ_COUNT" -eq "$OPEN_COUNT" ] \
  || fail "$OPEN_COUNT unconditional crate-root declaration(s) were found but only $READ_COUNT module prologue(s) were read. Every one of them must be examined; refusing to pass over a declaration that was skipped."

# Affirmative success line: a pasted gate SUMMARY must show that this check RAN, and
# every element of it is something the guard can only know AFTER enumerating the
# crate-root declarations and READING that many module files from disk. The
# `pub-surface` component of scripts/agent-gate.sh matches this line WHOLE (never a
# prefix — roborev r7 F3: a check against a PREFIX tests a SPELLING, not a STATE), so
# ANY change to its wording must be made in BOTH places, plus case 26(b)'s positive
# control in scripts/tests/test_pub_surface_guard.sh.
echo "pub-surface: $DECL_COUNT crate-root declarations scanned in $LIB_RS_REL ($MOD_COUNT pub mod, of which $OPEN_COUNT unconditional); $READ_COUNT module-file prologues read from source; $inconsistent inconsistent"
