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
    # A `cfg_attr` APPLIES its attributes only when its PREDICATE holds, and this scan
    # cannot evaluate a predicate. Propagating an inner GATED/HIDDEN outward therefore
    # asserted UNCONDITIONALLY something that is at best conditional — and both are
    # EXEMPTING verdicts, so `#[cfg_attr(any(), doc(hidden))] pub mod probe;` (neither
    # hidden nor gated: the predicate is FALSE) skipped its module file entirely and an
    # inner `#![cfg(...)]` passed undetected (roborev r11).
    #
    # So a `cfg_attr` is always OPEN — it never exempts. OPEN is the SAFE direction
    # here: it means "read the module file", which for a genuinely-hidden or
    # conditionally-gated module is harmless (a clean prologue still certifies) and for
    # a self-gating module correctly reports the defect. That is also exactly what case
    # 8 pins: a cosmetic `cfg_attr` must not silence the assert.
    #
    # Deliberately NOT a refusal: refusing would red `#[cfg_attr(docsrs, doc(hidden))]`,
    # a standard idiom, and a refusal that reds correct code is one agents learn to
    # waive. OPEN gets the same protection at no false-FAIL cost.
    return "OPEN"
  }
  return "OPEN"
}
function attrs_verdict(a,   i, j, c, d, m, v, res) {
  # TWO REFUSALS BEFORE ANY PARSING, and they must come FIRST because both defeat the
  # parsing itself. Note which direction is dangerous here: GATED and HIDDEN are the
  # EXEMPTING verdicts — only an OPEN declaration gets its module file read — so
  # anything that flips OPEN to GATED SKIPS INSPECTION and hides an inner-gated module.
  #
  # (1) RAW STRINGS (roborev r9 F1). `strip_strings` models only ORDINARY quoted
  #     strings, so a raw string's CONTENT leaks into the parsed structure: a cosmetic
  #     `doc = r##"", cfg(any()), ""##` exposes a comma and a `cfg(...)` to `split_meta`
  #     and can flip an unconditional declaration to GATED. Modelling raw-string hash
  #     counts is a second lexer — the class this guard has already paid for — so refuse.
  #     Anchored on a non-identifier boundary so an ordinary string ending in `r` (e.g.
  #     `doc = "for"`, which contains the two characters `r"`) does NOT match.
  #     ALL RAW PREFIXES, not just `r` (roborev r10): Rust also has raw BYTE strings
  #     `br#*"` and raw C strings `cr#*"`. The first version of this refusal matched
  #     only `r#*"`, and because `b`/`c` are identifier characters the boundary anchor
  #     made `br#"…"#` slip past the very check meant to stop it — the leak was
  #     narrower than the fix. `(b|c)?` closes it.
  if (a ~ /(^|[^A-Za-z0-9_])(b|c)?r#*"/) return "REFUSE_RAWSTRING"
  # ORDER IS LOAD-BEARING: strings are erased HERE, between the raw-string refusal and
  # the `path` test. The raw-string check must run on the RAW text (erasure cannot model
  # raw delimiters), but the `path` test must run on ERASED text — running it raw made
  # `#[doc = "the path = ..."]` read as a `#[path]` attribute and FAIL the full gate on
  # a perfectly ordinary doc attribute (roborev r11 F3). That is a FALSE FAIL, and a
  # refusal that reds correct code is the one agents learn to waive, which would devalue
  # every other refusal here.
  a = strip_strings(a)
  # (2) `#[path = "..."]` (roborev r9 F2). Module resolution assumes the two standard
  #     paths. With a DECOY `NAME.rs` present beside `#[path = "actual.rs"] pub mod
  #     NAME;`, the guard reads the decoy, finds it clean and certifies, while
  #     `actual.rs` carries the inner gate. Resolving `#[path]` (including its
  #     `cfg_attr`-applied form, and its directory-relative semantics) is real work;
  #     refusing is bounded and correct. Only `#[...]` spans reach here — a `///` doc
  #     comment terminates attribute collection — so a doc comment mentioning `path =`
  #     cannot trip this.
  if (a ~ /(^|[^A-Za-z0-9_])path[[:space:]]*=/) return "REFUSE_PATH"
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
    # UNBALANCED `#[`: previously this returned GATED, reasoning "do not exempt on
    # OPEN". That has the polarity backwards — GATED *is* an exemption here (the module
    # file is only read for OPEN), so an unreadable attribute was silently skipping the
    # very inspection it could not rule out. Refuse instead.
    if (j > length(a)) return "REFUSE_UNBALANCED"
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
  st = "code"; depth = 0; hashes = 0; bd = 0; pd = 0
  for (i = 1; i <= n; i++) {
    INCODE[i] = (st == "code") ? 1 : 0
    bmin = bd
    BRACE_START[i] = bd
    # PAREN/BRACKET depth at this line's START, carried ACROSS lines exactly as `bd`
    # already is (roborev r20 F2). A MACRO TOKEN TREE can span lines —
    # `swallow!(\n    pub mod phantom;\n);` is valid Rust emitting no module — and
    # single-line paren tracking could not see it, so Refusal I fired on the indented
    # `pub mod` inside the tree: the MANDATORY gate rejecting valid Rust.
    #
    # This is not a new lexer. normalize() ALREADY maintains cross-line state for
    # comments, strings, raw strings and BRACE depth; this completes a counter that
    # existed for one delimiter kind and not the others. Braces stay separate because
    # they carry ITEM SCOPE (`mod x { }`), which is a different question.
    PDEPTH_START[i] = pd
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
        if (c == "(" || c == "[") pd++
        else if (c == ")" || c == "]") { if (pd > 0) pd-- }
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
        # BLANK STRING CONTENTS, keep the delimiters and every column (roborev r13 F2).
        # Contents used to be copied VERBATIM, so a literal containing declaration-like
        # text leaked into the normalized source that BOTH derivations read. Derivation S
        # scans a line UNANCHORED while P is line-start anchored, so a one-line
        # `const X: &str = r#"pub mod fake;"#;` was found by S, missed by P, and the
        # cross-check reported a DISAGREEMENT — the mandatory gate REJECTING valid Rust.
        # A false FAIL, and the kind of refusal agents learn to waive.
        if (c == "\\") { out = out "  "; j += 2; continue }
        if (c == "\"") { out = out c; j++; st = "code"; continue }
        out = out " "; j++
      } else if (st == "raw") {
        if (c == "\"") {
          k = j + 1; cnt = 0
          while (substr(s, k, 1) == "#" && cnt < hashes) { cnt++; k++ }
          if (cnt == hashes) { out = out substr(s, j, k - j); j = k; st = "code"; continue }
        }
        out = out " "; j++
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
# root_depth_at(i, pos): is character `pos` of line `i` at CRATE-ROOT depth — brace
# depth 0 AND macro-delimiter depth 0 — walking from that line's start depths?
#
# THIS EXISTS BECAUSE FIVE SITES COMPUTED THIS INDEPENDENTLY AND DISAGREED. Reviews
# r19-r22 produced five findings, and the last two were not about behaviour at all: they
# were about two of those implementations not matching (Refusal Y tracked parens and
# brackets but not braces; Refusal U checked no delimiter depth at all). When findings
# stop saying "this is wrong" and start saying "these two do not match", a per-site patch
# is the wrong move BY CONSTRUCTION — the next site nobody touched produces finding six.
#
# So this is computed ONCE. It makes the whole class unreachable rather than smaller, and
# it REMOVES code rather than adding it, which is the test for whether a design is still
# sound. Safe to walk raw: normalize() has already blanked comments and string contents,
# so every delimiter seen here is real code.
#
# BRACE_MIN[] deliberately survives for Refusal I alone, where a LINE-MINIMUM
# over-approximation is the documented intent (`    } pub mod x;` must fire); its comment
# says so. Everywhere else, position-exact is what was always meant.
function root_depth_at(i, pos,   k, c, b, p, ln) {
  ln = N[i]
  b = BRACE_START[i]; p = PDEPTH_START[i]
  for (k = 1; k < pos; k++) {
    c = substr(ln, k, 1)
    if (c == "{") b++
    else if (c == "}") b--
    else if (c == "(" || c == "[") p++
    else if (c == ")" || c == "]") p--
  }
  return (b == 0 && p == 0)
}
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
  # NARROWED TO RESIDUE THAT COULD CARRY A DECLARATION. The original fired on ANY
  # non-blank residue, which the comment above accepted as safe over-approximation —
  # but it made the MANDATORY gate reject ORDINARY RUST: the closing line of a
  # multi-line raw string is `"#;`, and of a multi-line string `";`, so any crate root
  # containing one was refused outright. Found while testing the r13 F2 fix, not
  # reported. A refusal that reds correct code is the one agents learn to waive, which
  # would devalue every other refusal here, so the over-approximation is not free.
  #
  # The narrowing is still sound in the false-PASS direction, which is what matters: a
  # crate-root declaration is `pub mod NAME;` and therefore CANNOT exist without
  # LETTERS. Residue with no letter at all (`"#;`, `";`, `*/`, `)]`, `42;`) cannot be a
  # declaration, so skipping it cannot hide one. Residue containing any letter still
  # fires, `pub mod` or not — `*/ pub mod x;` and `"#; pub mod x;` both refuse.
  for (i = 1; i <= n; i++) {
    if (INCODE[i]) continue
    if (N[i] == "") continue
    if (N[i] !~ /[A-Za-z_]/) continue
    printf "X\tline %d: code follows a closing block-comment/string delimiter on the SAME line: `%s`\n", i, squash(substr(ltrim(N[i]), 1, 72))
  }

  # --- Refusal V: a depth-0 line that is nothing but a visibility qualifier -------
  #
  # `pub` NEWLINE `mod probe;` is VALID RUST — verified: it compiles — and EVERY scan
  # here requires `pub` and `mod` on the SAME line, so the declaration was invisible to
  # both derivations AND to Refusal U, and an inner-gated module passed unchecked
  # (roborev r15 F2). Both derivations blind together again, so the cross-check could not
  # see it either.
  #
  # Tokenizing declarations across newlines would be a second implementation of Rust's
  # item grammar — the class this guard exists to avoid. A bare visibility qualifier
  # alone on a line is UNAMBIGUOUS, though: it can only be a split declaration, so it is
  # refused by shape. Covers `pub`, `pub(crate)`, `pub(super)`, `pub(in path)`.
  for (i = 1; i <= n; i++) {
    if (!INCODE[i]) continue
    if (!BRACE_UNRELIABLE && BRACE_MIN[i] != 0) continue
    # THE LINE MUST *END* IN A DANGLING VISIBILITY TOKEN — not consist solely of one.
    # The first version matched only a whole line of `pub`, so `#[allow(dead_code)] pub`
    # followed by `mod probe;` slipped through (roborev r16). That is the SAME "pattern
    # narrower than the hole" shape as the macro refusal's line-anchored version, and I
    # wrote it one round AFTER recording the lesson — so it is stated here as a rule
    # rather than a note: MATCH THE FAMILY (a line ENDING in a dangling qualifier),
    # never the one reported spelling.
    #
    # Cannot false-fire on prose or literals: `normalize()` blanks comments AND string
    # contents. Cannot false-fire on an identifier ending in "pub" (`republic`) because
    # the token is boundary-anchored.
    if (N[i] !~ /(^|[^A-Za-z0-9_])pub([[:space:]]*\([^)]*\))?[[:space:]]*$/) continue
    # REQUIRE CORROBORATION FROM THE NEXT LINE (roborev r17). A dangling `pub` alone is
    # NOT sufficient evidence of a split declaration: a macro TOKEN TREE can legally
    # contain one, e.g. `const S: &str = stringify!(\n    pub\n);`, and refusing that
    # failed the MANDATORY gate on valid Rust that declares no module at all.
    #
    # So the refusal now fires only when the next non-blank in-code line BEGINS a module
    # declaration. That is corroboration, not grammar: two adjacent facts, neither of
    # which needs a parser. `stringify!`'s next line is `);`, so it certifies.
    #
    # This is Refusal V's SECOND correction (too narrow in r16, too broad in r17) and it
    # is the same lifecycle the item-macro refusal ran before being removed. The
    # difference, and the reason this one is kept: the split-declaration hole IS closable
    # by a bounded LOCAL rule, whereas separating an item macro from an expression macro
    # provably required item boundaries.
    # THE LOOKAHEAD MUST CROSS COMMENT-ONLY REGIONS (roborev r18 F2). It used to BAIL on
    # the first line not starting in ordinary code, so `pub`, then a MULTI-LINE comment
    # whose closing delimiter sits on its own line, then `mod probe;` defeated it —
    # missed by both derivations AND every refusal.
    #
    # Crossing them is safe rather than a new model: `normalize()` blanks comments AND
    # string contents to spaces, so a comment-body line, a closing-delimiter line and a
    # blank line are ALL whitespace-only in N[] and indistinguishable from each other. So
    # the rule is simply "skip whitespace-only lines, stop at the first with content" —
    # and a line with code AFTER a closing delimiter is Refusal X's business, not this
    # loop's.
    vnext = 0
    for (vj = i + 1; vj <= n; vj++) {
      if (N[vj] ~ /^[[:space:]]*$/) continue
      vnext = vj; break
    }
    if (vnext <= 0) continue
    # THE `mod` TOKEN IS ENOUGH — its identifier may be on a LATER line still (roborev
    # r19 F1): `pub` / `mod` / `probe;` across three lines was missed because this
    # required `mod NAME` together. Accepting a bare `mod` is a SIMPLIFICATION — one
    # fewer requirement, not one more — which is the direction that says this refusal
    # still has a correct bounded form and does not need deleting.
    if (N[vnext] !~ /^[[:space:]]*mod([[:space:]]|$)/) continue
    printf "V\tline %d: a depth-0 line ends in a bare visibility qualifier and the next line begins a module declaration, so the declaration is split across lines: `%s` / `%s`\n", i, squash(substr(ltrim(N[i]), 1, 40)), squash(substr(ltrim(N[vnext]), 1, 30))
  }

  # --- Refusal Y: an outer attribute AFTER other code on the same line ----------
  #
  # The structured scan recognises an attribute only at the START of a line, so
  # `const X: () = (); #[path = "actual.rs"]` followed by `pub mod probe;` DISCARDED the
  # `#[path]`: both scans then agreed `probe` was OPEN and resolution certified a clean
  # standard-path DECOY while the real, self-gated module went unexamined (roborev r21 F1).
  #
  # Same treatment as Refusals I and W, for the same reason: collecting a mid-line
  # attribute means a second rule underneath the derivations' primary collection rule,
  # where a defect becomes a blind spot they SHARE. Refuse instead.
  #
  # Scoped by DELIMITER DEPTH AT THE ATTRIBUTE, walked along the line, so an attribute
  # inside a parameter list (`fn f(#[allow(x)] a: u8)`) or inside a macro token tree is
  # untouched — those sit at depth > 0. MEASURED: zero such lines in the real lib.rs.
  for (i = 1; i <= n; i++) {
    if (!INCODE[i]) continue
    if (!BRACE_UNRELIABLE && BRACE_MIN[i] != 0) continue
    yline = N[i]
    for (yk = 1; yk <= length(yline) - 1; yk++) {
      yc = substr(yline, yk, 1)
      if (yc != "#") continue
      # BRACES TOO, via the shared helper (roborev r22 F1). This walk tracked parens and
      # brackets but not braces, so an attribute inside a one-line nested module —
      # `mod outer { const X: () = (); #[allow(dead_code)] fn f() {} }` — was misread as a
      # crate-root attribute and the mandatory gate rejected valid Rust.
      if (!root_depth_at(i, yk)) continue
      yrest = substr(yline, yk)
      if (yrest !~ /^#[[:space:]]*!?[[:space:]]*\[/) continue
      ybefore = substr(yline, 1, yk - 1)
      if (ybefore ~ /^[[:space:]]*$/) break      # attribute starts the line: not ours
      printf "Y\tline %d: an outer attribute FOLLOWS other code on the same line: `%s`\n", i, squash(substr(ltrim(yrest), 1, 60))
      break
    }
  }

  # --- Refusal W: an INDENTED attribute at crate-root depth ---------------------
  #
  # Both derivations skip indented lines — that column-zero rule is what keeps them
  # independent — so an INDENTED top-level attribute is invisible to both. It is not
  # inert, though: it can be `#[path = "actual.rs"]` or `#[cfg(...)]`, either of which
  # decides the verdict. Measured (roborev r20 F1): an indented `#[path]` above a
  # column-zero `pub mod probe;` was DISCARDED, the module read as attribute-free and
  # OPEN, and resolution certified a clean standard-path DECOY while the real, self-gated
  # file went unexamined.
  #
  # Refused rather than collected, for the same reason as Refusal I: teaching the
  # derivations to read indented attributes puts a second rule underneath their primary
  # collection rule, where a defect becomes a blind spot they SHARE. Scoped tightly —
  # brace depth 0 AND delimiter depth 0 — so attributes on struct fields, inside `mod`
  # blocks and inside macro token trees are all untouched. MEASURED: zero such lines in
  # cqlite-core/src/lib.rs, so it costs nothing today.
  for (i = 1; i <= n; i++) {
    if (!INCODE[i]) continue
    if (!BRACE_UNRELIABLE && BRACE_MIN[i] != 0) continue
    if (PDEPTH_START[i] > 0) continue
    if (N[i] !~ /^[[:space:]]+#[[:space:]]*!?[[:space:]]*\[/) continue
    printf "W\tline %d: an INDENTED attribute at crate-root depth: `%s`\n", i, squash(substr(ltrim(N[i]), 1, 72))
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
    # DEPTH-AWARE WITHIN THE LINE. S stays deliberately naive about ATTRIBUTES — that
    # independence from P is what makes the cross-check worth anything — but it must not
    # collect a declaration that is not at the CRATE ROOT. A one-line inline module,
    # `mod outer { pub mod inner; }`, is ordinary Rust in which `inner` is NOT a
    # crate-root declaration: P correctly ignored it, S collected it anyway, and the
    # cross-check called a DISAGREEMENT — the MANDATORY gate rejecting valid Rust.
    #
    # Case 24 already pinned that a nested `pub mod` stays green, but only in the
    # INDENTED multi-line form; the one-line form was uncovered. Found by probing, not
    # by review.
    #
    # Counting braces is not "modelling Rust": `normalize()` blanks comments AND string
    # contents, so the count is reliable, and Refusal I already depends on the same
    # BRACE data. S keeps its own collection RULE (an unanchored scan, no attribute
    # parsing), which is where its independence from P actually lives.
    # PAREN DEPTH TOO (roborev r19 F3). A macro token tree can contain a COMPLETE
    # declaration — `swallow!( pub mod phantom; );` is valid Rust that emits no module —
    # and brace depth alone reads it as crate-root, so S collected it, P did not, and the
    # cross-check called a disagreement. A false FAIL on the MANDATORY gate.
    #
    # One more counter, not one more model: the same technique as the brace count, over
    # text normalize() has already stripped of comments and string contents. Declarations
    # inside `(` … `)` are skipped; nothing at true crate-root depth changes.
    rest = t
    consumed = 0
    while (match(rest, /pub mod [A-Za-z_][A-Za-z0-9_]*[[:space:]]*;/)) {
      nm = substr(rest, RSTART + 8, RLENGTH - 8)
      sub(/[[:space:]]*;$/, "", nm)
      # Position-exact crate-root depth, via the shared helper — S used to carry its own
      # brace+paren counters, which is one of the five implementations this consolidates.
      spos = length(t) - length(rest) + RSTART
      if (root_depth_at(i, spos)) print "S\t" nm
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
      # POSITION-EXACT CRATE-ROOT DEPTH, via the shared helper. This loop searched the
      # whole column-zero line with NO depth check at all, so a nested inline module
      # `mod outer { pub mod inner {} }` — brace depth 1, not crate-root — was refused as
      # an unrecognised top-level form. Valid Rust rejected by the MANDATORY gate.
      #
      # THE SIXTH SITE. Reviews r19-r22 found five depth defects across five sites; a peer
      # predicted that patching them per-site would let "the next site nobody touched
      # generate finding six", and this is it — found by running the consolidation's own
      # regression sweep, not by a review. Which is the argument for the consolidation:
      # one implementation cannot disagree with itself.
      upos2 = length(t) - length(rest) + RSTART
      if (pre !~ /[A-Za-z0-9_]/ && tail !~ /^pub mod [A-Za-z_][A-Za-z0-9_]*[[:space:]]*;/ && root_depth_at(i, upos2)) {
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
    # NOT inside a macro token tree (roborev r20 F2): an indented `pub mod` inside
    # `swallow!( ... )` is token-tree CONTENT, not an indented crate-root declaration.
    if (PDEPTH_START[i] > 0) continue
    rest = t
    while (match(rest, /pub[[:space:]]+mod([^A-Za-z0-9_]|$)/)) {
      pre = (RSTART > 1) ? substr(rest, RSTART - 1, 1) : ""
      tail = substr(rest, RSTART)
      # POSITION-EXACT DEPTH (roborev r22 F2). This searched the whole line and checked
      # only the line-minimum brace depth, so a nested inline module
      # `mod outer { pub mod inner {} }` — and the same tokens inside a brace-delimited
      # macro — were rejected as unrecognised top-level declarations. Valid Rust, refused.
      upos = length(t) - length(rest) + RSTART
      if (pre !~ /[A-Za-z0-9_]/ && root_depth_at(i, upos)) {
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
        # CROSS COMMENT-ONLY REGIONS (roborev r19 F2). normalize() blanks comments to
        # SPACES, not "", and a line INSIDE a block comment carries INCODE 0 — so the old
        # `INCODE[nxt] && N[nxt] == ""` test stopped at BOTH. The attribute run was then
        # dropped and a CORRECTLY GATED module recorded as OPEN, i.e. the guard ACCUSED a
        # module of the very defect it does not have. A false FAIL, and the worst kind:
        # it indicts correct code by name.
        while (nxt <= n && N[nxt] ~ /^[[:space:]]*$/) nxt++
        if (nxt > n) break
        if (!INCODE[nxt]) break
        if (N[nxt] ~ /^[[:space:]]/) break
        CUR = nxt
        BUF = N[CUR]
      }
      # CANONICALISE WHITESPACE BETWEEN `#`, `!` AND `[` HERE TOO (roborev r18 F1).
      # r15 fixed this in the PROLOGUE reader and I did not carry it to the crate-root
      # scanner — the same pattern, fixed in ONE location instead of ALL of them. Left
      # alone, `# [path = "actual.rs"]` on the line before `pub mod probe;` was DISCARDED,
      # so both scans agreed the module was OPEN and resolution certified a clean
      # standard-path DECOY while the real, self-gated file went unexamined.
      #
      # Rule for this guard, since this is the second time one fix needed two homes: when
      # a LEXICAL assumption turns out to be wrong, grep for every site that shares it
      # before calling the fix done.
      if (BUF ~ /^#[[:space:]]*![[:space:]]*\[/) sub(/^#[[:space:]]*![[:space:]]*\[/, "#![", BUF)
      else if (BUF ~ /^#[[:space:]]*\[/) sub(/^#[[:space:]]*\[/, "#[", BUF)
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
if grep -q '^Y	' "$SCAN_RAW"; then
  echo "" >&2
  grep '^Y	' "$SCAN_RAW" | cut -f2- >&2
  fail "the crate root $LIB_RS_REL carries an outer attribute AFTER other code on the same line (see above). The structured scan recognises an attribute only at the START of a line, so such an attribute is DISCARDED — and it is not inert: \`#[path = \"...\"]\` redirects the module file and \`#[cfg(...)]\` gates the declaration. A \`#[path]\` hidden this way makes the guard resolve a clean standard-path DECOY while the real, self-gated module goes unexamined. Collecting mid-line attributes would put a second rule underneath the derivations' primary collection rule, where a defect becomes a blind spot they SHARE, so the guard refuses instead. Remedy: put the attribute on its own line at column zero."
fi

if grep -q '^W	' "$SCAN_RAW"; then
  echo "" >&2
  grep '^W	' "$SCAN_RAW" | cut -f2- >&2
  fail "the crate root $LIB_RS_REL carries an INDENTED attribute at crate-root depth (see above). Both of this scan's derivations skip indented lines — that column-zero rule is what keeps them independent — so such an attribute is invisible to BOTH, and it is not inert: \`#[path = \"...\"]\` redirects the module file and \`#[cfg(...)]\` gates the declaration. A \`#[path]\` hidden this way makes the guard resolve a clean standard-path DECOY while the real, self-gated module goes unexamined. Teaching the derivations to read indented attributes would put a second rule underneath their primary collection rule, where a defect becomes a blind spot they SHARE, so the guard refuses instead. Remedy: put the attribute at column zero."
fi

if grep -q '^V	' "$SCAN_RAW"; then
  echo "" >&2
  grep '^V	' "$SCAN_RAW" | cut -f2- >&2
  fail "the crate root $LIB_RS_REL splits a declaration across lines — a depth-0 line holds nothing but a visibility qualifier (see above). \`pub\` on one line and \`mod NAME;\` on the next is valid Rust, but every scan here requires them on the SAME line, so such a declaration is invisible to BOTH derivations and to Refusal U: they AGREE while both are blind, and a module whose file carries an inner \`#![cfg(...)]\` would be advertised unconditionally by the crate root while being absent from the compiled crate. Tokenizing item declarations across newlines would be a second implementation of Rust's item grammar, which is the defect class this guard exists to avoid, so it refuses instead. Remedy: put the declaration on one line."
fi

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
# MULTISETS, NOT SETS — `sort -u` here was a false PASS (roborev r10). The structured
# scan reads only the FIRST statement on a line, so a line carrying TWO declarations
# (`#[cfg(any())] pub mod probe; pub mod probe;` — valid Rust, because the first is
# configured OUT, so there is no duplicate definition) yields TWO simple-scan records
# and ONE structured record. Deduplication made those two derivations AGREE, and the
# UNCONDITIONAL second declaration was then never examined. Comparing multisets turns
# that into the existing "the two scans disagree" refusal, which is exactly the right
# verdict: a module set the guard cannot pin down.
grep '^S	' "$SCAN_RAW" | cut -f2- | LC_ALL=C sort >"$WORK_DIR/mods.simple"
cut -f2 "$DERIVED_MODS" | LC_ALL=C sort >"$WORK_DIR/mods.structured"
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
#    STATED RESIDUAL — MACRO-GENERATED CRATE-ROOT MODULES ARE NOT SEEN (#1712, #3366).
#    A macro can emit `pub mod NAME { #![cfg(...)] }`, and the literal declaration then
#    lives in the macro DEFINITION where neither derivation can reach it. A refusal for
#    this was built and REMOVED, deliberately, after four review rounds:
#      * function-like `name!(...)` invocations were refusable, but only by a pattern
#        broad enough to also hit ORDINARY EXPRESSION MACROS — `const X: bool =
#        cfg!(feature = "y");` at the crate root is valid Rust and was rejected.
#        Separating "item macro" from "expression macro" requires knowing ITEM
#        BOUNDARIES, i.e. parsing items;
#      * PROCEDURAL attribute and derive macros can emit a module with no `!` anywhere,
#        so no function-like pattern closes the class at all.
#    So the refusal reddened valid code while still not covering the hole. Measured: ZERO
#    crate-root item macros in cqlite-core/src/lib.rs, so the gap is a hardening
#    limitation, not a live hole. The principled fix is reachability from rustc's own
#    dep-info (#3366), which sees EXPANDED syntax and has no macro problem to model.
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
    # A FIRST-LINE SHEBANG (roborev r9 F3). rustc accepts `#!...` on line 1 when it is
    # not `#![` — VERIFIED with rustc 1.98.0: a module of `#!/usr/bin/env rust` +
    # `#![cfg(feature = "nope")]` compiles, and the gate APPLIES. Without this the
    # shebang reads as the first item, the prologue ends at line 1, and the file is
    # falsely certified. Exactly one is possible and only on line 1, so this is a
    # bounded, exact rule rather than a guess.
    # WHITESPACE IS LEGAL BETWEEN ATTRIBUTE TOKENS (roborev r15 F1). rustc accepts
    # `#! [cfg(feature = "x")]` — VERIFIED on rustc 1.98.0: it compiles and the gate
    # APPLIES (no symbol emitted) — while a contiguous `#![` test read it as a shebang on
    # line 1, or as the first item later, and CERTIFIED the module. So the `#!`/`[`
    # pairing is now recognised ACROSS whitespace, and anything else beginning with `#`
    # that this reader cannot classify REFUSES rather than being taken for prologue-end.
    #
    # Only two `#` shapes are legal here and they mean opposite things:
    #   `#` ws* `!` ws* `[`  -> an INNER attribute; keep reading it
    #   `#` ws* `[`          -> an OUTER attribute, i.e. the first item's; prologue ENDS
    # A shebang is `#!` NOT followed (across whitespace) by `[`, and only on line 1.
    if (t ~ /^#[[:space:]]*![[:space:]]*\[/) {
      sub(/^#[[:space:]]*![[:space:]]*\[/, "#![", t)   # canonicalise, then fall through
      L[i] = t
    } else if (t ~ /^#[[:space:]]*\[/) {
      printf "CLEAN\t%d\n", i                          # outer attribute: prologue ends
      exit
    } else if (i == 1 && t ~ /^#[[:space:]]*!/) {
      i++; continue                                     # line-1 shebang
    } else if (substr(t, 1, 1) == "#") {
      refuse(i, "a line begins with `#` in a shape this reader cannot classify as an inner attribute, an outer attribute or a shebang: `" squash(substr(t, 1, 72)) "`")
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
      # ALL raw-literal forms, ZERO-HASH INCLUDED (roborev r12). The first version tested
      # `r` followed by `#`, so a valid `r"..."` (no hashes) fell through to ordinary
      # string handling, where a trailing backslash is read as an escape and
      # `#![doc = r"\\"]` was REJECTED — a false FAIL. Same "pattern narrower than the
      # hole" shape as the declaration-side raw check. Token-boundary guarded so an
      # identifier ending in r/b/c does not match.
      prv = (p > 1) ? substr(buf, p - 1, 1) : ""
      if (prv !~ /[A-Za-z0-9_]/ && substr(buf, p) ~ /^(b|c)?r#*"/) {
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
    REFUSE_RAWSTRING)
      fail "\`pub mod $modname\` at $LIB_RS_REL:$lineno carries a declaration attribute containing a RAW STRING. This scan erases ordinary string contents before reading structure, but it does not model raw-string delimiters (\`r#*\"\`), so a raw string's CONTENT would leak into the parsed meta-items and could flip this declaration to an EXEMPT verdict — which would skip reading the module file entirely and hide an inner \`#![cfg(...)]\`. Refusing rather than guessing (roborev r9 F1). Remedy: use an ordinary string literal in the declaration's attributes." ;;
    REFUSE_PATH)
      fail "\`pub mod $modname\` at $LIB_RS_REL:$lineno carries a \`path\` attribute. This guard resolves a module to exactly one of its two STANDARD paths ($SRC_REL/NAME.rs or $SRC_REL/NAME/mod.rs); it does not follow \`#[path]\`. If a standard-path file also exists, the guard would read THAT file and certify while the real module file — the one \`#[path]\` names — carries the gate (roborev r9 F2). Refusing rather than examining the wrong file. Remedy: drop \`#[path]\` for this crate-root module, or extend this guard to resolve it." ;;
    REFUSE_UNBALANCED)
      fail "\`pub mod $modname\` at $LIB_RS_REL:$lineno carries a declaration attribute whose \`[\` never closes, so its attributes could not be read. An unreadable attribute is not an exemption: this guard cannot establish that the declaration is gated, so it must not skip reading the module file." ;;
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
# THE CENSUS CONTRACT LINE (#3162). This guard already walks its subject set and knows the
# number, so it states it in the ONE machine-readable shape the gate's component census reads
# — `AGENT-GATE-CENSUS: <n> <unit>` — instead of leaving this component a declared gap whose
# row reads `PASS (0s)` with nothing to affirm. The subject is the UNCONDITIONAL crate-root
# declarations actually verified against their module prologues, which is the property this
# guard exists to check. A zero there is not a legitimate outcome — the refusals above already
# fail a crate root with no unconditional declarations — so the census may safely treat zero as
# vacuity, and it cannot fire on correct input.
printf 'AGENT-GATE-CENSUS: %s unconditional crate-root pub mod declaration(s) verified against their module prologues\n' "$OPEN_COUNT"
echo "pub-surface: $DECL_COUNT crate-root declarations scanned in $LIB_RS_REL ($MOD_COUNT pub mod, of which $OPEN_COUNT unconditional); $READ_COUNT module-file prologues read from source; $inconsistent inconsistent"
