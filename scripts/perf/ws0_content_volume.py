#!/usr/bin/env python3
"""The ARROW CONTENT-VOLUME subject: how much payload the measured response carried.

Split out of `ws0_loadgen_record.py` under the campsite rule (source target ~800 lines) when
#3272's round-17/18 work pushed that file to 1010. The seam is by RESPONSIBILITY, and this is a
distinct one: `ws0_loadgen_record.py` is a CENSUS — it answers "which fields of the producer's
record does the reporting path account for at all", and its mechanism is a table walked against
the record. This module answers a single measurement question about ONE of those fields — how
much ARROW a response carried, what the rig can compare that against, and (round 18) why that
comparison is a self-consistency check rather than an oracle. It is the only subject on this path
whose expectation comes from a SEPARATE MEASUREMENT (the untimed preflight) rather than from a
constant or the session's configuration, which is why it needed a fifth disposition in the census
and why it reads a second artifact off disk.

Nothing about the split changes what is checked. The census still classifies `bytes_total`
`content-volume`, `_EXPECTATION_TABLES` still registers `CONTENT_VOLUME_INPUTS` against that
disposition, and `_CHECKED_DISPOSITIONS` still names `check_content_volume` as what reads it —
all three closures are asserted at import in `ws0_loadgen_record.py`, which imports from here, so
a table or a checker that went missing in the move is a REFUSAL AT IMPORT rather than a field
silently unverified.
"""

from __future__ import annotations

import json
import pathlib

from ws0_validate import Invalid, positive_int

# ============================================================================
# THE CONTENT-VOLUME INPUT — ROWS WERE VERIFIED, THE PAYLOAD WAS NOT (#3272 round 17)
# ============================================================================
# Every OTHER check on this record — all of them in `ws0_loadgen_record.py` — counts REQUESTS and
# ROWS. Not one of them looks at how much
# ARROW there was. `bytes_total` was classified `ignored` with the reason "the rig's claims are
# rows/s and cycles/row (spec R1); no byte-throughput figure is printed" — true, and insufficient
# in precisely the way the last five wrong reasons in that census were ("an INPUT", "not a
# measurement", "the loopback address", "a second source of truth"): a true statement about the
# FIELD standing in for a claim about the FIGURE. Whether a figure is PRINTED is not the test.
# The test the census states for itself is whether the field can change the VALIDITY of a printed
# figure — and this one can, more directly than any field yet found here.
#
# THE EXPOSURE. A `do_get` response carrying the expected NUMBER OF ROWS but FEWER ARROW COLUMNS
# (or narrower buffers) satisfies every existing check: `rows_total == requests_ok * corpus_rows`
# holds exactly, the request/error/shed counters are clean, the derived rate matches the recorded
# one, the tag and endpoint match the session. And it makes ARROW ENCODING LOOK FASTER, because
# the server encoded less. #3096 exists to measure Arrow-encode cost. So this defect flatters
# EXACTLY the quantity the parent issue set out to measure — and the rig's headline is a
# bare-scan-vs-Flight RATIO, so an asymmetric shortfall in one arm moves the published number
# directly rather than merely mislabelling it.
#
# WHY IT IS DERIVABLE AND SO IS VERIFIED RATHER THAN CONFESSED. `bytes` is summed client-side by
# `client.rs::do_get_drain` as `batch.get_array_memory_size()` per decoded `RecordBatch` — a
# function of the SCHEMA and the row count, not of timing, machine or load. It is therefore
# CONSTANT ACROSS EVERY FULL-CORPUS SCAN of a given corpus, and per-request it must be identical.
# Measured across all 40 committed loadgen records that carry both fields (#3217 partB counters,
# #3224 llc-s1/s6 steps, spanning concurrency 1..16 and 3..48 requests): `bytes_total /
# requests_ok` is 48,764,091,712 in EVERY ONE, with `bytes_total % requests_ok == 0` in every one,
# over the same 3,999,890-row full scan. So the per-scan Arrow extent is an OBSERVED INVARIANT of
# the corpus, not a hoped-for one.
#
# WHAT IS VERIFIED, AND FROM WHAT. The expectation is the rep's OWN UNTIMED PREFLIGHT — the warm
# prewarm leg, which `lib-measure.sh` already runs OUTSIDE the perf window and whose JSONL
# `ws0_prewarm` already requires to be a COMPLETE full-corpus scan (round 12's F2). That artifact
# is therefore a verified-complete observation of this corpus's Arrow extent, taken through the
# same server, before the timed window opened and at no cost to it. The timed requests must match
# it PER SCAN and EXACTLY:
#
#     bytes_total == requests_ok * (preflight bytes_total / preflight requests_ok)
#
# DERIVE, DON'T TRUST (round 12's F2 principle): the expectation comes from a separately-validated
# observation, never from the timed record itself. A record cannot certify its own payload.
#
# NO TOLERANCE, and no threshold. `get_array_memory_size()` is integer arithmetic over buffer
# capacities, so a legitimate full scan reproduces it bit-for-bit; a percentage band would be a
# number somebody chose, and the whole exposure here is a SHORTFALL, which a band lets through in
# the flattering direction by construction.
#
# WHAT THIS DOES NOT VERIFY, stated rather than left to be assumed: it pins the Arrow buffer
# EXTENT, not the SCHEMA identity. Two different column sets whose buffers happened to sum to the
# same capacity would pass. The oracle for that is the pinned `ARROW_BUFFER_DIGEST`
# (tools/ws0-corpus-gen/src/measurement_corpus.rs), which is folded per-column over validity
# bitmaps and value buffers and cannot be fooled that way — but NOTHING ON THIS PATH CAN REACH IT:
# `bytes` is the only thing `do_get_drain` retains from a batch before dropping it, so the loadgen
# record carries no per-column information at all, and making it carry any means changing
# `flight-loadgen`. That is outside this issue's scope. Successor work: have the loadgen fold the
# #3096 Arrow digest over the batches it drains and record it, so the rig can compare a rep's
# response against `ARROW_BUFFER_DIGEST` itself instead of against its total extent.
#
# ===== AND THE CLAIM IS WITHDRAWN: THIS IS NOT AN ORACLE (#3272 round 18) =====
#
# The paragraph above is where round 17 stopped, and it understated the defect by a category. It
# said the check cannot see a SCHEMA substitution. The finding is worse: it cannot see a SHORTFALL
# either — the very thing it was built to catch — whenever the shortfall is UNIFORM.
#
# The mechanism, stated precisely, because the distinction is subtle enough to have been praised as
# elegant before it was seen through. Deriving an expectation from an ALREADY-VERIFIED ARTIFACT is
# good practice and is not the defect. The defect is WHICH artifact: the untimed preflight is
# produced through the SAME TICKET, the SAME SERVER PROCESS and the SAME RESPONSE PATH as the timed
# requests it is asked to validate. So any omission that is a property of that path — a dropped
# column, a narrowed buffer, a projection the ticket resolves wrongly — is present in BOTH SIDES in
# EQUAL MEASURE, their byte counts AGREE, and the comparison passes while reporting a payload that
# is short. A self-consistency check standing in for an oracle.
#
# That is #3042's documented lesson, arrived at from a different direction: a CQLite-written +
# CQLite-read round trip is INVARIANT to a uniform framing error because both sides make the
# identical mistake. It is also the same shape round 14's F4 found in `verify_pinned_components`,
# which compared a component map against THE CORPUS'S OWN identity — both sides derived from the
# artifact under test. The rule the three share: an expectation and the thing it measures must not
# share a failure mode, and "it was verified" says nothing about that unless the VERIFIER was
# independent of the SUBJECT. `ws0_prewarm` verified the preflight's ROW COUNT against the pinned
# corpus — an independent oracle for rows, and no oracle at all for Arrow extent.
#
# WHY THE PINNED DIGEST CANNOT REPLACE IT (measured, not assumed). The obvious fix is to validate
# each timed response against `ARROW_BUFFER_DIGEST`, which IS independent: it is a literal in
# source, not an observation of this path. It is UNREACHABLE for this corpus, and the evidence is a
# run rather than a reading. `cqlite-flight/tests/issue_3096_arrow_buffer_digest.rs` reaches its
# digest comparison only THROUGH `assert_arms_agree` (fn at :764), which refuses any corpus whose
# taps observe ZERO NULL CELLS (:805) — pushing a failure and returning `None`, so the digest
# asserts in the measurement-corpus branch (:1175 onward) never execute. `ws0-corpus-gen` emits no
# nulls: `rows.rs::row_mutation` writes all nine non-key cells on every row unconditionally, with no
# `NullPlan` on that path at all. Run against a real 500-row generated corpus with all three
# expectation env vars supplied, the oracle FAILS at that refusal — "the producer tap on the merge
# arm observed ZERO null cells" — before comparing any digest. The refusal is correct on its own
# terms (a fold over absent validity bitmaps proves nothing about them); it simply means this
# corpus cannot be digest-checked. Note the refusal lives in the SHARED CALLEE, not in the
# measurement-corpus case body, so reading that case's body alone shows no null gate and suggests
# the digest is reachable. It is not.
#
# NOR IS THERE AN INDEPENDENT PINNED SUBSTITUTE, also measured. Two candidates were tested and both
# are toothless. (1) A bound derived from the pinned per-cell widths in `rows.rs`
# (`BLOB_A_LEN`/`BLOB_B_LEN`/`PAYLOAD_LEN`/... = 686 B/row of raw cell content) against the real
# recorded Flight figure of 12,191 Arrow bytes/row: a factor of 17.8 of slack, so it cannot fire on
# a 50% shortfall — Arrow buffer capacity is dominated by padding and offsets, not by cell content.
# (2) The observed per-scan extent 48,764,091,712 B recorded across 40 committed loadgen records: it
# is over a 3,999,890-row corpus, NOT the canonical 4,000,000-row one, so it is not a pin for the
# corpus this rig measures. Neither is a pinned quantity for this subject.
#
# SO THE CLAIM IS WITHDRAWN RATHER THAN RELABELLED-AND-KEPT-AS-VERIFICATION. The comparison is
# RETAINED — it is not worthless (see `CONTENT_VOLUME_ESTABLISHES` for exactly what it does and
# does not establish, and note it still refuses a ONE-SIDED shortfall, which is a real defect class)
# — but every name it reports under says SELF-CONSISTENCY, never `verified`, and the report output
# states the circularity in the reader's face. Round 16's F2 precedent: an honest partial with a
# named gap beats a claim the rig cannot support. Successor work is the same as above and is now the
# ONLY route to a real oracle here: have `flight-loadgen` fold the #3096 digest over the batches it
# drains, AND give `ws0-corpus-gen` a null plan so the digest oracle admits its corpus at all.
# Both need production changes outside this issue.
#
# WHAT THE RETAINED COMPARISON ESTABLISHES, AND WHAT IT DOES NOT (#3272 round 18).
#
# Reported in every rep's record beside the numbers, so a reader cannot take the comparison for a
# verification. Spelled once, here, because the report text and the record must not be able to
# describe it differently.
CONTENT_VOLUME_ESTABLISHES = (
    "ESTABLISHES: the timed response's Arrow payload volume AGREES with the untimed preflight's,"
    " per scan and exactly — so a shortfall affecting ONLY the timed requests (a mid-session"
    " server change, one rep served differently, a truncated stream) is still refused."
    " DOES NOT ESTABLISH: that either is COMPLETE. The preflight goes through the SAME ticket, the"
    " SAME server process and the SAME response path as the timed requests, so a uniform omission —"
    " a dropped column, a narrowed buffer — is present in both in equal measure, the byte counts"
    " AGREE, and this comparison passes on a payload that is short. It is a SELF-CONSISTENCY check,"
    " not an oracle: the expectation is not independent of the subject (#3042; the same shape as"
    " round 14's F4). It also cannot see a SCHEMA substitution, since it compares an extent rather"
    " than a per-column fold. The independent oracle would be the pinned ARROW_BUFFER_DIGEST, which"
    " is UNREACHABLE for this corpus: the #3096 digest oracle refuses a corpus observing zero null"
    " cells and ws0-corpus-gen emits none."
)

CONTENT_VOLUME_INPUTS: dict[str, tuple[str, str, str]] = {
    "bytes_total": (
        "this rep's own UNTIMED PREFLIGHT (`<tag>.prewarm.jsonl`), through the SAME ticket, server"
        " and response path — a SELF-CONSISTENCY reference, NOT an independent oracle (#3272 round"
        " 18), scaled to this rep's requests_ok",
        "the ARROW PAYLOAD VOLUME of the measured response. Every other check on this record"
        " counts REQUESTS and ROWS, so a response carrying the expected number of rows and FEWER"
        " ARROW COLUMNS — or narrower buffers — satisfied all of them: the rows are an exact"
        " multiple of the corpus count, the counters are clean, the derived rate matches the"
        " recorded one. And it makes ARROW ENCODING LOOK FASTER, because the server encoded less."
        " #3096 exists to measure Arrow-encode cost and the rig's headline is a bare-scan-vs-"
        "Flight RATIO, so this defect flattered exactly the quantity the measurement was for."
        " It was classified IGNORED as `no byte-throughput figure is printed`, which is true and"
        " is not the test: the test is whether the field can invalidate a figure that IS printed"
        " (#3272 round 17)",
        "THE MEASURED RESPONSE CARRIED A DIFFERENT ARROW PAYLOAD than the verified-complete"
        " preflight scan of this same corpus through this same server. A SHORT payload means the"
        " server encoded less Arrow than the report's cycles/row is divided by, which makes"
        " Arrow encoding look CHEAPER — the one quantity #3096 exists to measure — and moves the"
        " published bare/Flight ratio in the flattering direction. Re-run the rep; do not report"
        " a response whose payload volume disagrees with a complete scan of the corpus.",
    ),
}


# WHAT THE SESSION HAS NO ORACLE FOR, NAMED IN THE OUTPUT (#3272 round 17).
#
# The preflight the expectation comes from is the WARM prewarm leg, and `lib-measure.sh` skips the
# prewarm on the COLD arm BY DESIGN — a prewarm there would make "cold" meaningless. So a COLD-ONLY
# session legitimately contains no preflight at all, and there is no way to manufacture one without
# destroying the thing it would verify.
#
# That is a real gap and it is RECORDED rather than skipped. The branch is keyed on the AFFIRMATIVE
# presence of the oracle, never on the absence of a bad signal, and when it is absent the rep's
# record carries THIS STRING instead of a verified block — so a reader sees the check did not run
# rather than reading a silence as a pass. Round 16's F2 precedent: an honest partial with a named
# gap beats a check that cannot fire, and beats a claim the rig cannot support.
#
# ROUND 18 CORRECTION. This constant's name and the note below describe the COLD-ONLY case, where
# there is no preflight to compare against at all. That case is unchanged. But the WARM case is not
# "verified" either — its reference is not independent of its subject (see the note above
# `CONTENT_VOLUME_INPUTS`), so BOTH branches now report an absence of independent verification and
# differ only in whether a self-consistency comparison RAN. The sentinel below says "not compared";
# the running branch says "compared, self-consistently, which is not verification".
CONTENT_VOLUME_NO_ORACLE = "NOT COMPARED — no untimed preflight in this session"

CONTENT_VOLUME_NO_ORACLE_NOTE = (
    "The measured response's ARROW PAYLOAD VOLUME (`bytes_total`) was NOT verified for this rep."
    " The expectation is one verified-complete full-corpus scan's payload, taken from the UNTIMED"
    " prewarm leg — and lib-measure.sh skips the prewarm on the COLD arm by design, because a"
    " prewarm would make `cold` meaningless. So a cold-only session has no oracle for this"
    " property and one cannot be synthesised without destroying what it would verify. What that"
    " leaves unverified, stated plainly: a response carrying the expected ROW COUNT with FEWER"
    " ARROW COLUMNS, or narrower buffers, would satisfy every other check on this rep and would"
    " make Arrow encoding look CHEAPER — the quantity #3096 exists to measure. A session that"
    " includes a WARM arm runs a SELF-CONSISTENCY comparison against that arm's preflight, which is"
    " NOT a verification: the preflight uses the same ticket, server and response path, so a uniform"
    " shortfall appears on both sides and passes (#3272 round 18). Successor work: have"
    " flight-loadgen fold the #3096 Arrow-buffer digest over the batches it drains and record it"
    " per step, so a rep's response can be compared against the pinned ARROW_BUFFER_DIGEST"
    " directly — that also closes the SCHEMA-identity half this extent check cannot reach, and it"
    " needs no prewarm, so it would cover the cold arm too. That route ALSO requires giving"
    " ws0-corpus-gen a null plan: the #3096 digest oracle refuses a corpus whose taps observe zero"
    " null cells, and this generator emits none, so the pinned digest is currently unreachable for"
    " this corpus even with a per-step digest recorded."
)


def preflight_arrow_bytes_per_scan(session_dir: pathlib.Path) -> float | None:
    """The Arrow payload volume of ONE VERIFIED-COMPLETE full-corpus scan (#3272 round 17).

    THE UNTIMED PREFLIGHT, and it already exists. `lib-measure.sh` runs a prewarm leg per WARM rep
    OUTSIDE the perf window and retains its JSONL at `<tag>.prewarm.jsonl`; `ws0_prewarm` already
    refuses to call that leg `ok` unless EVERY successful request streamed the PINNED corpus row
    count (round 12's F2). So the rig already possesses, at zero cost to the timed measurement and
    with no change to `cqlite-flight` or `flight-loadgen`, a validated-complete observation of this
    corpus's Arrow extent taken through the same server — exactly the expectation the timed
    requests must match.

    Resolved at SESSION level, over EVERY preflight present, not per rep. The Arrow extent of a full
    scan is a function of the SCHEMA and the ROW COUNT alone (`client.rs::do_get_drain` sums
    `batch.get_array_memory_size()`), so it is invariant across reps, arms and temperatures of one
    corpus — which is why a cold rep can be checked against a warm rep's preflight, and why every
    preflight in the session MUST AGREE. A disagreement is refused: two different payload volumes
    for the same corpus means at least one of them is not what this report thinks it measured.

    Returns bytes PER SCAN (`bytes_total / requests_ok`), or `None` when the session holds no
    preflight at all — a COLD-ONLY session, where the prewarm is skipped by design. `None` is a
    NAMED gap the caller records in the output (`CONTENT_VOLUME_NO_ORACLE`), never a silent skip.

    Operands go through `positive_int`, never a bare `int()`: this value MULTIPLIES the expectation
    for every timed rep, so a truncated or boolean operand would silently move the bar rather than
    being refused (the #3272 R6/B5 class).
    """
    per_scan: dict[float, str] = {}
    for path in sorted(session_dir.glob("*.prewarm.jsonl")):
        try:
            records = [
                json.loads(line) for line in path.read_text().splitlines() if line.strip()
            ]
        except (OSError, ValueError) as exc:
            raise Invalid(
                f"the untimed preflight {path.name} is not readable JSONL ({exc}), so the"
                " verified-complete Arrow payload volume it is the record of cannot be read. An"
                " unparseable oracle is a refusal, never a skipped comparison (#3272 round 17)."
            ) from None
        if not records:
            raise Invalid(
                f"the untimed preflight {path.name} holds no step record, so it observed nothing"
                " and cannot state this corpus's Arrow payload volume (#3272 round 17)"
            )
        ok = 0
        total = 0
        for idx, rec in enumerate(records):
            for key in ("requests_ok", "bytes_total"):
                if key not in rec:
                    raise Invalid(
                        f"the untimed preflight {path.name} record {idx} carries no `{key}`, so"
                        " the Arrow payload volume of a complete scan was NOT OBSERVED and cannot"
                        " be asserted. A missing operand is an error, never an assumed default —"
                        " defaulting it would make the comparison pass precisely when the"
                        " preflight is silent about it (#3272 round 17)."
                    )
            ok += positive_int(
                f"preflight {path.name} record {idx} requests_ok",
                rec["requests_ok"],
                "The preflight's Arrow volume is divided by it to get bytes PER SCAN, so a"
                " non-positive or fractional count would move the expectation every timed rep is"
                " measured against.",
            )
            total += positive_int(
                f"preflight {path.name} record {idx} bytes_total",
                rec["bytes_total"],
                "It IS the verified-complete Arrow payload volume every timed rep's response is"
                " compared against; a zero means the preflight streamed no Arrow at all.",
            )
        if total % ok != 0:
            raise Invalid(
                f"the untimed preflight {path.name} streamed {total:,} Arrow bytes over {ok}"
                " successful request(s), which is not a whole number per scan. Every request in a"
                " verified-complete preflight scanned the SAME corpus, and the client's per-batch"
                " `get_array_memory_size()` sum is a function of the schema and the row count"
                " alone — so a remainder means those requests did not all carry the same payload,"
                " and no single per-scan volume describes them. Re-run rather than reporting"
                " against an expectation that averages unequal responses (#3272 round 17)."
            )
        per_scan.setdefault(total / ok, path.name)
    if not per_scan:
        return None
    if len(per_scan) != 1:
        detail = ", ".join(
            f"{name} = {v:,.0f} B/scan" for v, name in sorted(per_scan.items())
        )
        raise Invalid(
            "this session's untimed preflights DISAGREE about the Arrow payload volume of a"
            f" full-corpus scan: {detail}. That volume is a function of the SCHEMA and the ROW"
            " COUNT alone (the client sums `get_array_memory_size()` per decoded batch), so one"
            " corpus has exactly one value and a disagreement means at least one preflight did not"
            " scan what this report thinks it measured. No single expectation can be derived from"
            " these, so none is invented — re-run the session (#3272 round 17)."
        )
    return next(iter(per_scan))


def check_content_volume(
    tag: str, rec: dict, requests_ok: int, expected_per_scan: float
) -> dict:
    """REQUIRE the measured response to carry the PAYLOAD a complete scan carries (#3272 round 17).

    `bytes_total` was classified IGNORED because "no byte-throughput figure is printed" — true, and
    not the test. Every other check on this record counts REQUESTS and ROWS, so a response with the
    expected number of rows and FEWER ARROW COLUMNS (or narrower buffers) passed all of them, and
    it makes ARROW ENCODING LOOK FASTER because the server encoded less. #3096 exists to measure
    Arrow-encode cost, and the rig's headline is a bare-scan-vs-Flight RATIO, so the defect
    flattered exactly the quantity being measured.

    NOT A VERIFICATION — a SELF-CONSISTENCY CHECK, and the name of every key it returns says so
    (#3272 round 18). `expected_per_scan` comes from `preflight_arrow_bytes_per_scan`, which is a
    separately-validated observation (so this record cannot certify its own payload — that part
    holds) but is NOT INDEPENDENT OF THE SUBJECT: it traverses the same ticket, server process and
    response path, so a UNIFORM shortfall appears identically on both sides and passes here. What it
    still catches is a ONE-SIDED shortfall. See the module note above `CONTENT_VOLUME_INPUTS` for the
    mechanism, the measured evidence that the pinned `ARROW_BUFFER_DIGEST` cannot be reached for this
    corpus, and the successor work.

    Compared EXACTLY, with no tolerance and no threshold. `get_array_memory_size()` is integer
    arithmetic over buffer capacities, so a legitimate full scan reproduces it bit-for-bit — and
    the exposure is a SHORTFALL, which a percentage band would admit in the flattering direction by
    construction. `expected_per_scan` is a float only because it is a quotient; the product is
    compared against the integer `bytes_total` after an exact-integer check on the expectation, so
    no float rounding decides a verdict.

    Returns what was verified, so the rep's record can state it.
    """
    source, why, consequence = CONTENT_VOLUME_INPUTS["bytes_total"]
    if "bytes_total" not in rec:
        raise Invalid(
            f"flight rep {tag} step record carries no `bytes_total`, so the ARROW PAYLOAD VOLUME"
            " of the measured response was NOT OBSERVED and cannot be asserted. A missing field is"
            f" an error, never an assumed default (#3272 round 17). {why}"
        )
    observed = positive_int(
        f"flight rep {tag} bytes_total",
        rec["bytes_total"],
        "It is the ARROW PAYLOAD VOLUME of the measured response — the evidence that the server"
        " encoded the Arrow this rep's cycles/row is divided by. A zero means it encoded none.",
    )
    want = expected_per_scan * requests_ok
    if want != int(want):
        raise Invalid(
            f"internal: rep {tag}'s expected Arrow volume ({expected_per_scan!r} per scan x"
            f" {requests_ok} request(s) = {want!r}) is not a whole number of bytes, so no exact"
            " comparison is possible. The per-scan figure is validated as an exact quotient by"
            " preflight_arrow_bytes_per_scan; reaching this means that guard was bypassed."
        )
    if observed != int(want):
        short = int(want) - observed
        direction = (
            f"{short:,} bytes SHORT ({100.0 * short / want:.4f}% less Arrow than a complete scan"
            " carries — the flattering direction)"
            if short > 0
            else f"{-short:,} bytes MORE than a complete scan carries"
        )
        raise Invalid(
            f"flight rep {tag} streamed {observed:,} Arrow bytes over {requests_ok}"
            f" successful request(s), but {source} is {expected_per_scan:,.0f} bytes per scan, so"
            f" this rep should have carried {int(want):,} — it is {direction}."
            f" {why}. {consequence}"
        )
    # KEYED AS SELF-CONSISTENCY, NOT AS VERIFICATION (#3272 round 18). The keys are the claim: a
    # consumer reading `bytes_total_verified` would be reading an assertion this comparison cannot
    # support, because the reference comes from the same ticket/server/response path as the subject
    # (see the module note above). Renamed rather than supplemented — a consumer of the old key must
    # FAIL to find it and come read what replaced it, rather than keep reading a value whose meaning
    # silently weakened under an unchanged name (the `forced_merge_path` rule from round 16).
    return {
        "bytes_total": observed,
        "bytes_total_verified_against_independent_oracle": False,
        "bytes_per_scan_observed": observed / requests_ok,
        "bytes_per_scan_self_consistent_with": expected_per_scan,
        "bytes_per_scan_reference_source": source,
        "scope": CONTENT_VOLUME_ESTABLISHES,
    }
