#!/usr/bin/env python3
"""The driver's CONTIGUOUS ENVIRONMENT-ASSIGNMENT PREFIX for one embedded step.

Two modes, one subject:

    <driver> <VAR-PREFIX> <BLOCK-NEEDLE>                 "<in-prefix> <total-in-file>"
    <driver> <VAR-PREFIX> <BLOCK-NEEDLE> --emit-prefix   the validated prefix TEXT

Extracted from `test_ws0_embedded_steps_execute.sh` so both the membership check and
`driver_step_env` call ONE implementation — a second copy would drift, and then the eval
would be bounded by a validation that is not the one that passed.
"""

import bisect, pathlib, re, sys

# THIS file's directory, so the shipped joiner/census are imported rather than re-implemented and
# the caller does not have to know where they live.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from ws0_embedded_python import _join_continuations, census

path = pathlib.Path(sys.argv[1])
prefix, needle = sys.argv[2], sys.argv[3]
text = path.read_text()

# WHICH block consumes this prefix, by the shipped writer its body calls.
records, _findings = census(path)
owners = [r for r in records if r["kind"] == "BLOCK" and needle in r["body"]]
if len(owners) != 1:
    print(f"AMBIGUOUS: {len(owners)} block(s) call {needle!r}", file=sys.stderr)
    raise SystemExit(2)
invocation_line = owners[0]["line"]

joined, omap = _join_continuations(text)
# The ORIGINAL offset where that invocation's physical line begins.
line_starts = [0] + [i + 1 for i, ch in enumerate(text) if ch == "\n"]
if invocation_line > len(line_starts):
    print(f"UNRESOLVABLE: line {invocation_line} is past end of file", file=sys.stderr)
    raise SystemExit(2)
invocation_offset = line_starts[invocation_line - 1]

# The JOINED logical line covering it. Spans are in scan space; their bounds are mapped back
# through `omap`, so the comparison is against original offsets throughout.
spans, start = [], 0
for i, ch in enumerate(joined):
    if ch == "\n":
        spans.append((start, i))
        start = i + 1
spans.append((start, len(joined)))
logical = None
for a, b in spans:
    if b <= a or a >= len(omap):
        continue
    if omap[a] <= invocation_offset <= omap[b - 1]:
        logical = joined[a:b]
        break
if logical is None:
    print(f"UNRESOLVABLE: no logical line covers offset {invocation_offset}", file=sys.stderr)
    raise SystemExit(2)

names = sorted(set(re.findall(prefix + r"[A-Z_]+(?==)", text)))

# THE PROPERTY IS THE CONTIGUOUS ENVIRONMENT-ASSIGNMENT PREFIX, NOT MERE MEMBERSHIP OF THE
# LOGICAL LINE (#3451 post-rebase round 1, F1). `n + "=" in logical` passed for
#
#     WS0_CFG_BASELINE_MODE="$BASELINE_MODE"; python3 -c '...'
#
# which keeps the assignment on the SAME logical line while bash makes it a standalone shell
# variable python never receives. MEASURED, both directions:
#
#     WS0_CFG_REPS="1"; python3 -c ...   -> os.environ.get(...) is None
#     WS0_CFG_REPS="1"  python3 -c ...   -> "1"
#
# So the text BEFORE the command word must be a run of assignment words and nothing else. A
# command separator anywhere in it, or any word that is not NAME=, means the run is broken and
# the membership answer is worthless — reported as zero present, which FAILS closed rather than
# guessing which side of the break each name fell on.
# THE PREFIX IS THE LEADING RUN OF ASSIGNMENT WORDS, found by WALKING them — not by locating the
# string "python3". Searching for the command word by name truncates the prefix at the first
# assignment whose VALUE happens to contain it: measured, adding `WS0_CFG_BIN_HINT="python3"`
# to the driver dropped the answer to `present=1/14` on an otherwise correct prefix, which is
# the false-red direction. Walking the words asks the property directly and cannot be fooled by
# a value.
all_words = logical.split()
prefix_words = []
for w in all_words:
    if not re.match(r"[A-Za-z_][A-Za-z0-9_]*=", w):
        break
    prefix_words.append(w)
head = " ".join(prefix_words)
# THE FIRST NON-ASSIGNMENT TOKEN MUST **BE** THE CONSUMING COMMAND (#3451 post-rebase round 5,
# F1). Requiring merely that SOME non-assignment token exists let a STANDALONE `;` satisfy it:
#
#     WS0_CFG_X=... ; python3 -c '...'
#
# the `;` is its own word, so it breaks the loop before reaching `head`, the separator scan never
# sees it, and `len(prefix_words) < len(all_words)` is satisfied by `;` and `python3` between
# them. Reported contiguous, every name present — while production leaves the assignments
# unexported, and the suite stayed green because it evaluates them directly before `env`.
#
# Round 1's F1 caught the ATTACHED form (`NAME="v";`, still an assignment word, so the separator
# scan below finds it). This is the DETACHED one, and naming the expected next token subsumes
# both: anything between the assignments and the command — an operator, a redirect, another
# command — means these assignments are not that command's environment.
following = all_words[len(prefix_words)] if len(prefix_words) < len(all_words) else ""
contiguous = following == "python3" and not any(ch in head for ch in ";&|")
if contiguous:
    present = [n for n in names if any(w.startswith(n + "=") for w in prefix_words)]
else:
    present = []
if "--resolve" in sys.argv[4:]:
    # THE PREFIX IS PARSED AND SUBSTITUTED, NEVER EVALUATED (#3451 post-rebase round 7, F3).
    #
    # Round 2 authorised an `eval` on the argument that the contiguity check bounded its input:
    # "only NAME= words with no command separator". MEASURED, that check admits
    # `WS0_CFG_X=$(helper)`, `WS0_CFG_X=`helper`` and `WS0_CFG_X=${OTHER}` — all
    # assignment-shaped, none containing a separator. Command substitution walked straight
    # through the stated bound, so a driver line of that shape would have executed
    # repository-derived shell inside a test documented as hermetic and read-only, in a mandatory
    # gate component. The precondition did not exclude the dangerous case.
    #
    # So: a RESTRICTED GRAMMAR, allowlisted at every level. A value is a sequence of literal
    # characters drawn from an explicit set, plus `$VAR` / `${VAR}` references resolved from the
    # caller's controlled table. Command substitution, backticks, arithmetic, parameter-expansion
    # operators, redirections, globs and any form not named here are REFUSED BY NAME — a failure,
    # never a skip. Nothing is executed, so the header's "hermetic" is a fact rather than a hope.
    if not contiguous:
        print(f"NOT-A-PREFIX: the first non-assignment token is {following!r}, not the consuming"
              " `python3` command", file=sys.stderr)
        raise SystemExit(4)
    table = {}
    for pair in sys.argv[4:]:
        if pair == "--resolve":
            continue
        name, _, value = pair.partition("=")
        table[name] = value
    LITERAL = set(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        "/.:,-_+@% "
    )
    VARNAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

    def resolve(word):
        name, _, raw = word.partition("=")
        if raw[:1] == '"':
            if not raw.endswith('"') or len(raw) < 2:
                raise ValueError(f"{name}: unbalanced double quote in {raw!r}")
            raw = raw[1:-1]
        elif raw[:1] == "'":
            raise ValueError(f"{name}: single-quoted values are not in the supported grammar")
        out, i = [], 0
        while i < len(raw):
            ch = raw[i]
            if ch == "$":
                rest = raw[i + 1 :]
                if rest[:1] == "{":
                    m2 = VARNAME.match(rest, 1)
                    if not m2 or rest[m2.end() : m2.end() + 1] != "}":
                        raise ValueError(
                            f"{name}: only a plain ${{VAR}} reference is supported, not"
                            f" {raw[i:i + 12]!r} (parameter-expansion operators are refused)"
                        )
                    var, i = m2.group(0), i + 1 + m2.end() + 1
                elif rest[:1] == "(":
                    raise ValueError(
                        f"{name}: command substitution/arithmetic is REFUSED — this is the"
                        " construct the round-2 safety argument wrongly assumed was excluded"
                    )
                else:
                    m2 = VARNAME.match(rest)
                    if not m2:
                        raise ValueError(f"{name}: a bare $ is not a supported reference")
                    var, i = m2.group(0), i + 1 + m2.end()
                if var not in table:
                    raise ValueError(f"{name}: ${var} is not in the controlled table")
                out.append(table[var])
                continue
            if ch not in LITERAL:
                raise ValueError(
                    f"{name}: {ch!r} is not a supported literal character (backticks,"
                    " redirections, globs and quoting forms are refused)"
                )
            out.append(ch)
            i += 1
        return name, "".join(out)

    try:
        resolved = [resolve(w) for w in prefix_words]
    except ValueError as exc:
        print(f"UNSUPPORTED-ASSIGNMENT: {exc}", file=sys.stderr)
        raise SystemExit(5)
    for name, value in resolved:
        print(f"{name}={value}")
    raise SystemExit(0)
if "--emit-prefix" in sys.argv[4:]:
    # THE VALIDATED PREFIX TEXT, printed ONLY when the run above is intact. The caller evaluates
    # it (see `driver_step_env`), and this conditionality is the whole safety argument: the text
    # handed over has already been proved to be assignment words with no command separator.
    if not contiguous:
        print(f"NOT-A-PREFIX: the first non-assignment token is {following!r}, not the consuming"
          " `python3` command", file=sys.stderr)
        raise SystemExit(4)
    print(head)
    raise SystemExit(0)
print(f"{len(present)} {len(names)}")
