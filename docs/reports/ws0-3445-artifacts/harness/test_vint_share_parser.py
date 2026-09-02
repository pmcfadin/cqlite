#!/usr/bin/env python3
"""Tests for the addr2line inline-chain parser (issue #3445).

Exists because of a specific defect, and the first case is that defect: `addr2line -i -f`
answers an address it cannot resolve with the literal `??` / `??:0`, and an earlier parser
appended the `??` function line to the chain. `["??"]` is TRUTHY, so unresolved samples were
classified as "not VInt" rather than "unknown", `no_chain_cycles` was 0 BY CONSTRUCTION, and
the refusal threshold meant to catch that undercount could never fire. A false zero in a
validity guard is worse than no guard, so the parser is pinned here rather than trusted.

Run: python3 test_vint_share_parser.py
"""
import importlib.util
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("vint_share", HERE / "vint_share.py")
vs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vs)

FAILURES = []


def check(name, got, want):
    if got == want:
        print(f"ok   {name}")
    else:
        print(f"FAIL {name}\n       got  {got!r}\n       want {want!r}")
        FAILURES.append(name)


def fake_addr2line(payload: str):
    """Run the real parser against a canned addr2line payload.

    The payload is fed through a stub `addr2line` on PATH, so the parser under test is
    exercised exactly as it runs in production -- including its argv and its subprocess
    handling -- rather than by calling an inner helper the real code path might bypass.
    """
    d = tempfile.mkdtemp()
    stub = pathlib.Path(d) / "addr2line"
    stub.write_text("#!/bin/sh\ncat <<'EOF'\n" + payload + "\nEOF\n")
    stub.chmod(0o755)
    env_path = f"{d}:{__import__('os').environ['PATH']}"
    orig = subprocess.run
    def patched(cmd, **kw):
        e = dict(kw.pop("env", None) or __import__("os").environ)
        e["PATH"] = env_path
        return orig(cmd, env=e, **kw)
    subprocess.run = patched
    try:
        return vs.inline_chains("/nonexistent-binary", [0x1000])
    finally:
        subprocess.run = orig


# 1. THE DEFECT: a wholly unresolved address must yield an EMPTY chain, not ["??"].
check("unresolved address -> empty chain",
      fake_addr2line("0x0000000000001000\n??\n??:0"),
      {0x1000: []})

# 1b. THE ROUND-2 RESIDUAL (roborev r3 finding 1): function `??` with a REAL location. The
#     previous parser dropped a frame only when BOTH halves were unknown, so this came back as
#     ["??"] -- truthy -- bypassing the no-chain guard while discarding the one component that
#     could identify the code. Both halves must survive.
check("?? function with a real location keeps the location",
      fake_addr2line("0x0000000000001000\n??\n/data/x/cqlite-core/src/parser/vint.rs:41"),
      {0x1000: [("??", "/data/x/cqlite-core/src/parser/vint.rs:41")]})

# 1c. ...and such a chain must classify as VInt, not as "other": no usable function anywhere,
#     but a location inside the decoder's own source file.
check("?? function + vint.rs location classifies as narrow",
      vs.classify([("??", "/data/x/cqlite-core/src/parser/vint.rs:41")]), "narrow")

# 1d. A chain with nothing usable classifies as UNRESOLVED, which callers route to no_chain.
check("wholly unusable chain -> unresolved", vs.classify([]), "unresolved")
check("?? + ??:0 frame -> unresolved", vs.classify([("??", "??:0")]), "unresolved")

# 2. A real inlined chain keeps every frame, innermost first.
check("resolved chain keeps all frames",
      fake_addr2line(
          "0x0000000000001000\n"
          "cqlite_core::parser::vint::decode_unsigned\n"
          "/src/parser/vint.rs:41\n"
          "cqlite_core::parser::vint::parse_vuint\n"
          "/src/parser/vint.rs:418"),
      {0x1000: [("cqlite_core::parser::vint::decode_unsigned", "/src/parser/vint.rs:41"),
                ("cqlite_core::parser::vint::parse_vuint", "/src/parser/vint.rs:418")]})

# 3. The `<u8>::leading_ones` case this attribution depends on: an innermost frame in core
#    whose OUTER frame is the decoder. Both are kept, so `classify` still sees the decoder.
check("core frame inlined into the decoder is retained",
      fake_addr2line(
          "0x0000000000001000\n<u8>::leading_ones\n/rustc/x/library/core/src/num/uint_macros.rs:201\n"
          "cqlite_core::parser::vint::decode_unsigned\n/src/parser/vint.rs:44"),
      {0x1000: [("<u8>::leading_ones", "/rustc/x/library/core/src/num/uint_macros.rs:201"),
                ("cqlite_core::parser::vint::decode_unsigned", "/src/parser/vint.rs:44")]})

# 4. A frame with a real function but an unknown LINE is information and is kept: dropping it
#    would swing the error the other way and undercount attributable cycles.
check("known function with unknown line is kept",
      fake_addr2line("0x0000000000001000\ncqlite_core::parser::vint::decode_unsigned\n??:0"),
      {0x1000: [("cqlite_core::parser::vint::decode_unsigned", "??:0")]})

# 5. Mixed batch: a resolved address and an unresolved one must not contaminate each other.
check("unresolved entry does not leak into its neighbour",
      fake_addr2line(
          "0x0000000000001000\ncqlite_core::parser::vint::decode_unsigned\n/src/parser/vint.rs:41\n"
          "0x0000000000002000\n??\n??:0"),
      {0x1000: [("cqlite_core::parser::vint::decode_unsigned", "/src/parser/vint.rs:41")],
       0x2000: []})

# 6. classify() must agree: an empty chain is NOT "other" as far as callers are concerned --
#    they test emptiness first. Pinned so a future refactor cannot fold it back in.
check("classify treats a decoder frame as narrow",
      vs.classify([("cqlite_core::parser::vint::decode_unsigned", "/src/parser/vint.rs:41")]),
      "narrow")
check("classify treats a bare vint-module frame as wide_only",
      vs.classify([("cqlite_core::parser::vint::parse_vuint", "/src/parser/vint.rs:418")]),
      "wide_only")
# A usable function that is NOT vint must stay "other" even if some location mentions vint.rs:
# a real function name is never overridden by the location rescue.
check("usable non-vint function is not rescued by a vint location",
      vs.classify([("malloc", "/data/x/cqlite-core/src/parser/vint.rs:41")]), "other")

# 7. The unresolved-frame predicate itself, at its boundaries.
# ---------------------------------------------------------------------------------------
# roborev r4: the location rescue must be correct in BOTH directions, per FRAME.
# ---------------------------------------------------------------------------------------

# F3a TOO NARROW (regression case): an unresolved DECODER frame with a RESOLVED outer caller.
# The previous rule tested "no usable function anywhere in the chain", so an ordinary resolved
# caller suppressed the rescue -- the common case, not a corner one.
# A frame outside vint.rs entirely, with no usable function, is "other" -- the rescue must not
# reach beyond the decoder's own file.
check("unresolved frame in an unrelated file -> other",
      vs.classify([("??", "/x/cqlite-core/src/storage/other.rs:12")]), "other")

check("unresolved decoder frame + resolved outer caller -> narrow",
      vs.classify([("??", "/x/cqlite-core/src/parser/vint.rs:41"),
                   ("<cqlite_core::...::V5CompressedLegacyParser>::parse_cell_value_schema_order",
                    "/x/cqlite-core/src/storage/.../cell_value.rs:99")]),
      "narrow")

# F3b TOO BROAD (must REFUSE to rescue): an unresolved frame in vint.rs OUTSIDE the decoder
# line ranges is some other function in the same file -- `parse_vint`, a length helper, the
# encode path -- and is NOT narrow decode work.
# It is NOT narrow. It is `wide_only`: the location proves module membership (this is
# `parse_vint` / a length helper), which is exactly what the WIDE boundary is defined as. Booking
# it "other" would undercount wide; booking it "narrow" would overcount the decoder.
check("unresolved NON-decoder line in vint.rs -> wide_only, NOT narrow",
      vs.classify([("??", "/x/cqlite-core/src/parser/vint.rs:102")]), "wide_only")
check("unresolved encode-path line in vint.rs -> wide_only, NOT narrow",
      vs.classify([("??", "/x/cqlite-core/src/parser/vint.rs:120")]), "wide_only")

# ...while the three decoder ranges DO rescue.
for ln in (40, 73, 79, 82, 255, 257):
    check(f"unresolved vint.rs:{ln} (decoder range) -> narrow",
          vs.classify([("??", f"/x/cqlite-core/src/parser/vint.rs:{ln}")]), "narrow")

# A location whose line number is unparseable must NOT be rescued on a guess.
# Unparseable line: file known, function unknown, so the widest supportable claim is module
# membership -- never narrow, and never rescued on a guessed line number.
check("unresolved vint.rs with unparseable line -> wide_only, NOT narrow",
      vs.classify([("??", "/x/cqlite-core/src/parser/vint.rs:abc")]), "wide_only")

# A resolved non-vint function is never overridden by a decoder-range location.
check("resolved non-vint function is not rescued by a decoder location",
      vs.classify([("malloc", "/x/cqlite-core/src/parser/vint.rs:41")]), "other")

check("_loc_in_decoder: decoder line", vs._loc_in_decoder("/x/cqlite-core/src/parser/vint.rs:41"), True)
check("_loc_in_decoder: non-decoder line", vs._loc_in_decoder("/x/cqlite-core/src/parser/vint.rs:102"), False)
check("_loc_in_decoder: other file", vs._loc_in_decoder("/x/cqlite-core/src/parser/other.rs:41"), False)

check("_func_usable: ??", vs._func_usable("??"), False)
check("_func_usable: real", vs._func_usable("foo"), True)
check("_loc_usable: ??:0", vs._loc_usable("??:0"), False)
check("_loc_usable: ??:?", vs._loc_usable("??:?"), False)
check("_loc_usable: real", vs._loc_usable("/src/a.rs:9"), True)

print()
if FAILURES:
    print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
    sys.exit(1)
print("all parser tests passed")

# ---------------------------------------------------------------------------------------
# CALLER ATTRIBUTION (vint_regions.caller_of) — roborev r4 regression.
# Chains became (function, location) pairs and this consumer still searched the TUPLE, so
# every caller shipped as "?". These cases would have caught it.
# ---------------------------------------------------------------------------------------
_vr_spec = importlib.util.spec_from_file_location("vint_regions", HERE / "vint_regions.py")
vr = importlib.util.module_from_spec(_vr_spec)
_vr_spec.loader.exec_module(vr)

DECODER = ("cqlite_core::parser::vint::decode_unsigned", "/x/cqlite-core/src/parser/vint.rs:41")
ADAPTER = ("cqlite_core::parser::vint::parse_vuint", "/x/cqlite-core/src/parser/vint.rs:418")
REAL_CALLER = ("<cqlite_core::...::V5CompressedLegacyParser>::parse_row_metadata",
               "/x/cqlite-core/src/storage/.../row_framing.rs:240")

check("caller_of finds the innermost non-vint cqlite_core frame",
      vr.caller_of([DECODER, ADAPTER, REAL_CALLER]), REAL_CALLER[0])
check("caller_of does NOT return '?' for a well-formed pair chain (the r4 regression)",
      vr.caller_of([DECODER, ADAPTER, REAL_CALLER]) != "?", True)
check("caller_of returns '?' when no non-vint cqlite_core frame exists",
      vr.caller_of([DECODER, ADAPTER]), "?")
# Defect 1: core helpers inlined INTO the decoder are CALLEES, never callers.
check("caller_of ignores <u64>::swap_bytes (a callee, not a caller)",
      vr.caller_of([("<u64>::swap_bytes", "/rustc/x/core/num/mod.rs:1"), DECODER, REAL_CALLER]),
      REAL_CALLER[0])
check("caller_of ignores <[u8]>::copy_from_slice (a callee)",
      vr.caller_of([("<[u8]>::copy_from_slice", "/rustc/x/core/slice/mod.rs:1"), DECODER, REAL_CALLER]),
      REAL_CALLER[0])

print()
if FAILURES:
    print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
    sys.exit(1)
print("all parser + caller tests passed")
