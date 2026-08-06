#!/usr/bin/env python3
"""THE FLIGHT TICKET AS A VERIFIED MEASUREMENT INPUT (#3272 review round 10, M1).

Split out beside `ws0_schema_input.py` under the campsite rule, and by the same seam — one
question per module about whether a measurement means what it says:

    ws0_session.py        the CORPUS's identity   — which DATA was measured?
    ws0_schema_input.py   the SCHEMA's identity    — which SCHEMA was it read WITH?
    ws0_ticket_input.py   the TICKET's identity    — which REQUEST was measured?

# The finding

`ticket-template.json` is the Flight arm's REQUEST: keyspace, table, DDL, token range, column
projection, predicates, aggregation and limit. `flight-loadgen` re-reads it on EVERY invocation
(`--ticket-template`), on the prewarm leg and on the measured leg, for every rep of every arm.

It was created AFTER the session corpus was pinned, and it appeared in NO verified record. So it
could be modified between reps, or between arms, WITHOUT INVALIDATING CORPUS IDENTITY: every
digest the report re-derives still agrees, the pin still matches, and the report exits 0 while two
arms measured DIFFERENT QUERIES. A ratio between an arm answering `SELECT *` and an arm answering
a projected, predicated or LIMITed query compares nothing.

That is the same class as round 10's F-B one layer out. F-B was the two arms ingesting different
CORPORA; this is the two arms answering different REQUESTS. Both are invisible to every check that
looks at the corpus, because in both cases the corpus is untouched.

# The mechanism, and why it EXTENDS the pin rather than adding a second one

The session pin (`session-corpus-pin.json`) already exists, is already stamped BEFORE the first
rep, and is already REQUIRED by `ws0_report.py`. So the ticket digest is recorded there —
`ticket_template_sha256`, a top-level pin field beside `schema_sha256` — and re-derived from disk
at report time by [`verify_pinned_ticket`], which `verify_session_corpus_pin` calls unconditionally.
A second parallel mechanism would be a second thing to keep in step, which is how F3's component
map came to be written and read by nothing.

The digest is computed by the WRITER at pin time (there is nowhere else it could come from — see
below) and re-derived at report time from the bytes on disk. Cheap in both places: the template is
a few hundred bytes, so unlike `Data.db` there is no cost argument for a skip flag, and therefore
there is none.

# THE ONE ASYMMETRY VS THE SCHEMA, stated because it changes what the check can establish

`ws0-events.cql` is written by the CORPUS GENERATOR, which records its digest in
`corpus-identity.json`. So `verify_pinned_schema` compares THREE values — pin, recorded identity,
bytes on disk — and can therefore distinguish an edited identity file from edited data.

The ticket template is written by the DRIVER, from the corpus's DDL, at session setup. No recorded
identity carries it and none can: it does not exist until a session starts. So this check compares
TWO values — the pin and the bytes on disk — and that is the strongest available statement, not a
weaker version of the schema's. It establishes exactly this: THE REQUEST THIS SESSION WAS STARTED
AGAINST IS THE REQUEST STILL ON DISK.

# What it does NOT establish, recorded rather than left to be assumed

A modification that is REVERTED before reporting is not caught — the same residual the schema and
the DDL carry, for the same reason (both are re-read per invocation while the comparison happens
once, at reporting). Closing it would mean re-verifying inside the rep loop, i.e. a second call
site for this same function; it is not closed here because report-time is where every other
measurement input is re-derived, and a session whose inputs were churned and restored is not a
failure mode anyone has produced. What is closed is the whole of the finding: a template CHANGED
between reps or between arms, which by construction persists to report time.

# BACKWARD COMPATIBILITY: an absent digest is a REFUSAL, not a skip

A session dir stamped by a driver that predates this field has no `ticket_template_sha256`. Such a
pin is REFUSED. An absent digest means the request was never pinned, which is the condition the
finding is about — and treating "no record" as "nothing to check" is the fail-open shape: a check
that did not run prints exactly like one that passed. The remedy is to re-run the session with
`scripts/perf/ws0-baseline.sh`, and the refusal says so.

# WHERE THE TICKET LIVES: THE SESSION DIR, NOT THE CORPUS (#3272 round 13, F2)

M1 above brought the ticket into the provenance guarantee and wrote it into the SHARED CORPUS
DIRECTORY. That is the wrong owner, for two independent reasons:

* CONCURRENT SESSIONS COLLIDE. A corpus is a 2.8 GB read-only-by-nature artifact that two lanes
  routinely measure at the same time (that is the point of generating one and keeping it). Both
  drivers wrote `<corpus>/ticket-template.json`, so session B's write landed BETWEEN session A's
  pin and session A's reps. If the shape happened to differ, A's reps read B's request and A's
  report refused at the end (a wasted multi-minute run whose diagnosis names a mid-session
  mutation nobody performed); if the shape happened to be identical, the digests agreed and the
  collision was silent — a guarantee that holds by luck. Either way the pin's claim ("the request
  this session STARTED against is the request still on disk") was not a property of the session.
* IT MADE AN IMMUTABLE CORPUS WRITABLE. A corpus that nothing writes to can be mounted read-only,
  chmod'ed `a-w`, or shared between users. Requiring a write into it for every session is a
  needless coupling: the ticket is a property of the SESSION (the driver composes it, at setup,
  from the corpus's DDL), not of the corpus.

So the ticket now lives in the session's OUTPUT DIRECTORY, which `lib-outdir.sh`'s `claim_out_dir`
has already claimed EXCLUSIVELY (an atomic `mkdir` marker; a concurrent claim is a refusal, not a
race). That is the same ownership move round 12's F2 made for the measured binaries — they are
COPIED into a session-owned `measured-bin/` and hashed AT THE DESTINATION — and this follows that
precedent rather than inventing a second mechanism: the ticket is WRITTEN at the destination and
hashed there, so the digest is always of bytes inside the claimed directory.

What that changes for the reader: `ticket_path` and `verify_pinned_ticket` take the SESSION DIR.
The pin, the ticket and the reps' `--ticket-template` argument therefore all name one path inside
one exclusively-owned directory, and no other session can reach it.
"""

from __future__ import annotations

import json
import pathlib

from ws0_session import PIN_TICKET_FIELD, sha256_file
from ws0_validate import Invalid, _SHA256_RE

# The name the DRIVER writes and `flight-loadgen --ticket-template` reads. Not configurable here:
# both the driver and this check resolve exactly this path, and a ticket at a different path is a
# different request.
TICKET_FILENAME = "ticket-template.json"

# The pin field, RE-EXPORTED from `ws0_session` rather than spelled again: that module owns the
# pin's shape, and two spellings of one key would present as an absent-field refusal on a session
# that pinned the ticket correctly.
PIN_FIELD = PIN_TICKET_FIELD


def ticket_path(session_dir: pathlib.Path) -> pathlib.Path:
    """The ticket inside the SESSION-OWNED output directory (#3272 round 13, F2).

    Takes the SESSION dir, not the corpus. `claim_out_dir` has already claimed that directory
    exclusively, so this path cannot be written by a concurrent session — and the corpus needs no
    write permission at all. See the module docstring for the collision that made this necessary.
    """
    return session_dir / TICKET_FILENAME


# The connector-shaped `FlightTicket` the loadgen sends, with the DDL filled in. Everything except
# the DDL is a FIXED FULL-RING `SELECT *`: no projection, no predicate, no filter, no aggregation,
# no limit and no snapshot. That shape is the measurement — a projected or LIMITed request would
# measure a different query — so it is written here as data rather than composed at the call site,
# where a future edit could vary it per arm.
_TICKET_SHAPE: dict = {
    "version": 2,
    "keyspace": "ws0",
    "table": "events",
    "snapshot": None,
    "token_start": None,
    "token_end": None,
    "wraparound": False,
    "columns": None,
    "predicates": [],
    "filter": None,
    "aggregation": None,
    "limit": None,
}


def write_ticket_template(session_dir: pathlib.Path, ddl_file: pathlib.Path) -> str:
    """Write `ticket-template.json` INTO THE SESSION DIR from the corpus's own DDL; return its digest.

    Lives HERE rather than as a heredoc in the driver (#3272 round 10, M1): the module that decides
    whether the request is the one that was measured is the module that should own what the request
    IS. The driver's job is the ORDER — this before the pin — which stays visible at its top level.

    The DDL is read from the file whose digest the driver has just verified against the recorded
    corpus identity, so the request and the data are anchored to one schema.

    The DESTINATION is the SESSION's exclusively-claimed output directory, never the shared corpus
    (#3272 round 13, F2): two lanes measuring one corpus used to overwrite each other's request
    between the pin and the reps, and the corpus had to be writable for no reason. Written at the
    destination and hashed there — the precedent round 12's F2 set for the frozen binaries — so the
    digest can only ever describe bytes inside the claimed directory.

    Returns the digest so the caller can print it; the PIN takes its own measurement from the file
    (`measure_ticket_digest`) rather than trusting a returned value — one implementation, and a
    digest the pin records is always one the pin observed.
    """
    try:
        ddl = ddl_file.read_text().strip().rstrip(";")
    except OSError as exc:
        raise Invalid(
            f"the corpus DDL {ddl_file} cannot be read ({exc}), so the Flight request cannot be"
            " generated. Both arms read this schema; see ws0_schema_input."
        ) from None
    if not ddl:
        raise Invalid(
            f"the corpus DDL {ddl_file} is EMPTY, so the Flight ticket would carry no schema at"
            " all — the request would be unanswerable and a rep would fail for a reason unrelated"
            " to what is being measured."
        )
    path = ticket_path(session_dir)
    try:
        # The session dir exists by the time the driver calls this (`create_out_dir` +
        # `claim_out_dir` run far above), but `parents=True` keeps the fixture path — which builds a
        # session dir directly — from needing a second spelling of the same setup.
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({**_TICKET_SHAPE, "ddl": ddl}, indent=1))
    except OSError as exc:
        raise Invalid(
            f"{path} could not be written ({exc}), so this session cannot pin WHICH REQUEST it"
            " measures."
        ) from None
    return measure_ticket_digest(session_dir)


def measure_ticket_digest(session_dir: pathlib.Path) -> str:
    """The digest of the ticket template ON DISK, for the pin writer.

    An ABSENT or unreadable template is `Invalid`, never an absent pin field: the Flight arm
    cannot run without it, so a session pinned without one could only ever produce a report about
    a request nobody recorded.
    """
    path = ticket_path(session_dir)
    if not path.is_file():
        raise Invalid(
            f"{path} does not exist, so this session cannot pin WHICH REQUEST it is about to"
            " measure. `flight-loadgen --ticket-template` re-reads this file on every invocation"
            " of every rep of every arm, so an unpinned request can be changed between arms —"
            " making two arms answer DIFFERENT QUERIES while every corpus digest still agrees"
            " (#3272 M1). The driver writes it from the corpus's own DDL before pinning; if it is"
            " absent, that step did not run."
        )
    try:
        return sha256_file(path)
    except OSError as exc:
        raise Invalid(
            f"{path} exists but cannot be read ({exc}), so the request this session measures"
            " cannot be pinned."
        ) from None


def verify_pinned_ticket(
    pin_path: pathlib.Path, pin: dict, session_dir: pathlib.Path
) -> dict:
    """Compare the PINNED ticket digest against the bytes on disk (#3272 round 10, M1).

    Called unconditionally by `verify_session_corpus_pin` — not behind a flag, because a request
    check that can be switched off is the fail-open shape one level out, and the file is a few
    hundred bytes so a skip could only buy a vacuous green.

    Two values, not three, and that is a property of the input rather than a gap: the template is
    written by the driver at session setup, so no recorded corpus identity carries it (see the
    module docstring).

    Reads the ticket from the SESSION DIR (#3272 round 13, F2). It used to read the shared corpus,
    where a concurrent session's write landed between this session's pin and its reps: identical
    shapes agreed silently (a guarantee held by luck) and differing ones refused a correct run for a
    mutation nobody performed. The session dir is claimed exclusively, so neither is reachable.
    """
    pinned = pin.get(PIN_FIELD)
    if pinned is None:
        raise Invalid(
            f"{pin_path} records no `{PIN_FIELD}`, so the REQUEST this session measured is not"
            " pinned by anything. `ticket-template.json` carries the keyspace, table, DDL, token"
            " range, column projection, predicates, aggregation and limit, and"
            " `flight-loadgen --ticket-template` re-reads it on EVERY invocation — so a template"
            " modified between reps or between arms makes the arms answer DIFFERENT QUERIES while"
            " the corpus is untouched and every recorded digest still agrees. Nothing else in this"
            " report can see that, because nothing else looks at the request (#3272 M1). This"
            " session dir was stamped by a driver that predates the field: re-run the session with"
            " scripts/perf/ws0-baseline.sh, which creates the template BEFORE the pin and records"
            " its digest in the pin."
        )
    if not isinstance(pinned, str) or not _SHA256_RE.match(pinned):
        raise Invalid(
            f"{pin_path}: `{PIN_FIELD}` is {pinned!r}, which is not 64 lowercase hex characters —"
            " a truncated pin cannot identify the request this session measured"
        )
    path = ticket_path(session_dir)
    if not path.is_file():
        raise Invalid(
            f"the pinned Flight ticket {path} is MISSING, but the session pinned its digest"
            f" ({pinned}). Every Flight rep in this session read that file, so a session whose"
            " request no longer exists cannot be reported: there is nothing left to establish"
            " WHICH QUERY the figures describe."
        )
    try:
        measured = sha256_file(path)
    except OSError as exc:
        raise Invalid(
            f"the pinned Flight ticket {path} cannot be read: {exc}. The pin names it as the"
            " request this session measured, so an unreadable ticket means the session cannot be"
            " reported."
        ) from None
    if measured != pinned:
        raise Invalid(
            f"THE FLIGHT TICKET CHANGED under this session. {path} hashes to {measured} but the"
            f" session pinned {pinned} before the first rep. This file IS THE REQUEST — keyspace,"
            " table, DDL, token range, column projection, predicates, aggregation, limit — and"
            " `flight-loadgen --ticket-template` re-reads it on EVERY invocation of EVERY rep of"
            " EVERY arm. So a modification mid-session means the reps did not all measure the same"
            " query, and two arms compared head-to-head may have answered different ones: a"
            " projected or LIMITed request against a `SELECT *` one produces a ratio that compares"
            " nothing. NO other check in this report can see this — the corpus is untouched, so"
            " every corpus digest, the component set and the schema all still agree (#3272 M1)."
            " Re-run the session."
        )
    return {
        "pinned_ticket_sha256": pinned,
        "ticket": str(path),
        "ticket_bytes": path.stat().st_size,
        "note": (
            "the Flight ticket (the REQUEST: keyspace/table/DDL/token range/projection/"
            "predicates/aggregation/limit) was created BEFORE the corpus pin, its digest recorded"
            " in the pin, and re-derived here from the bytes on disk — always, with no skip flag."
            " Compared against the pin and the disk only: the driver writes this file at session"
            " setup, so no recorded corpus identity carries it and none can. What that establishes"
            " is that the request this session STARTED against is the request still on disk; a"
            " modification reverted before reporting is not covered, the same residual the"
            " per-invocation DDL carries. It lives in this SESSION's exclusively-claimed output"
            " directory, not in the shared corpus (#3272 F2), so a concurrent session measuring the"
            " same corpus cannot write it — and the corpus itself needs no write permission"
        ),
    }
