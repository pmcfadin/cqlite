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
