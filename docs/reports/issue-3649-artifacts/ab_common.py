"""
Anchored, sanitized emission shared by every #3649 analysis module.

WHY THIS IS ITS OWN MODULE
--------------------------
This report gets pasted into a GitHub issue. Every line it writes -- stdout AND
stderr, from any module -- must begin with `AB-3649: `, so that no run of it can
be mistaken for, or grepped as, a gate or review certification. Putting the two
emit functions in one place is what makes that a property of the whole harness
rather than a habit each module has to remember. Nothing in this artifact set
writes to a stream except through `out()` and `err()`.

The static text of every module here is asserted free of the reserved
gate/review marker strings by `selftest-analyze.sh`.
"""

import os
import sys

PREFIX = "AB-3649: "


def sanitize(text):
    """Render control characters visible so no value can break the anchor.

    Git permits newlines in paths and an operator may put anything in a manifest
    field; unsanitized, such a value emits a line with no prefix at all and
    breaks the one anchor everything else rests on. Values are otherwise printed
    verbatim -- masking a substring would mangle it for the reader.
    """
    named = {"\n": "\\n", "\r": "\\r", "\t": "\\t"}
    chunks = []
    for ch in str(text):
        code = ord(ch)
        if ch in named:
            chunks.append(named[ch])
        elif code < 0x20 or code == 0x7F:
            chunks.append("\\x%02x" % code)
        else:
            chunks.append(ch)
    return "".join(chunks)


def out(line=""):
    sys.stdout.write(PREFIX + sanitize(line) + "\n")


def err(line=""):
    sys.stderr.write(PREFIX + sanitize(line) + "\n")


# THE DOCUMENTED FLOORS, IN ONE PLACE, READ BY BOTH SIDES.
#
# These are not defaults an operator may lower for a measurement -- they are the
# conditions under which a target-band verdict means anything. The #3058
# single-source bypass has now been reachable three separate ways (a recursive
# census, a symlinked decoy, and simply passing `--min-sstables 1`), and the
# third route existed because the analyzer trusted the threshold the SESSION
# UNDER TEST reported instead of the documented minimum. A verdict must not
# derive its validity from a number its own subject chose.
#
# Lowerable only under an explicit `--control` label, where the verdict is
# already disclaimed.
MIN_CORPUS_BYTES_FLOOR = 268435456
MIN_SSTABLES_FLOOR = 2


class Unmeasured(Exception):
    """Every input the harness cannot measure. Carries a NAMED cause.

    A cause is a short kebab-case token; the detail is prose. Both are printed,
    the cause on its own `cause` line so an operator can act on it without
    reading the sentence.
    """

    def __init__(self, cause, detail):
        super().__init__(cause)
        self.cause = cause
        self.detail = detail


def pair_order(replicate):
    """Which arm runs FIRST in this replicate's pair.

    Executable, and therefore testable, for the reason round 1 paid for: the rule
    used to be three lines inline in the session loop, which needs a rig, so
    nothing could run it. This is the one rule in the driver whose failure mode is
    a CONFIDENT WRONG ANSWER rather than an error -- if base always ran first, a
    monotonic drift within a pair would land on the head arm every time and bias
    every ratio in one direction, and every test of the statistics would still
    pass. A rule like that must not be the untested one.

    Base first on odd replicates, head first on even ones, so over an even count
    each ordering runs exactly half the time.
    """
    return ("base", "head") if replicate % 2 == 1 else ("head", "base")


# LIVES HERE because BOTH the driver support and the analyzer's input layer need
# it: the driver decides the order and `ab_input` validates that the order the
# records show is the order the rule declares. Putting it in either one made the
# other import it, and the two already import each other -- so the shared rule
# belongs in the shared module rather than in whichever consumer wrote it first.


def _canonically_within(root_real, target_real):
    """MIRRORS `canon_target.starts_with(&canon_root)` (pathsafe.rs:117).

    COMPONENT-WISE, not string-wise: Rust's `Path::starts_with` compares path
    COMPONENTS, so `/a/bc` does NOT start with `/a/b`. A bare `str.startswith`
    would accept it, which is a containment check that admits a sibling
    directory whose name merely shares a prefix.
    """
    if target_real == root_real:
        return True
    return target_real.startswith(root_real.rstrip(os.sep) + os.sep)


# LIVES HERE for the same reason pair_order does: the driver's corpus
# enumeration and the analyzer's run-file provenance need the SAME containment
# rule, and the two modules already import each other. A second implementation
# of "is this path inside that one" is a second place for the component-wise
# subtlety to be got wrong.
