#!/usr/bin/env python3
"""Extract the job facts `roborev-review.sh` asserts on, out of a roborev JSON payload.

Usage:  roborev-job-facts.py <job-id> <facts-out> <prompt-out>   # payload on stdin

Exit 0 when the job was located and a facts file was written; 1 otherwise (the
wrapper then treats the structured data as unavailable and says so in its block).

Why this is its own file (issue #2964): the wrapper is a shell script and this is
JSON decoding — including a field that is DOUBLY encoded — so keeping it inline cost
~75 lines of embedded python in a file already over the campsite size guidance. One
responsibility per file: locate the job, normalise its fields, hand back `key=value`
lines a shell can read with `sed`.

TOKEN ACCOUNTING — the part that has already broken twice:

* `token_usage` is a JSON-ENCODED STRING in the real payload, so it must be decoded
  TWICE. Reading it as a nested object silently yielded no counts at all, which the
  wrapper reported as `UNAVAILABLE` — i.e. the corroborating vacuity tier was DEAD
  CODE on every real run.
* the output count is `total_output_tokens`, not `output_tokens`.

So this script accepts a documented ALIAS SET per count, and — critically — reports
a `token_state` so the wrapper can tell three cases apart:

    absent       no token field at all           -> legitimately UNAVAILABLE
    parsed       counts readable                 -> the tier evaluates
    unparseable  a token field IS present but no alias resolved to a number
                 -> EXTERNAL-TOOL DRIFT. This is the dangerous one: before it was
                    distinguished, any upstream rename or a `null` value degraded
                    the tier to a non-failing UNAVAILABLE while the underlying
                    counts were the recorded vacuous baseline, and the run PASSED.

`verdict` is reported because it is the STRUCTURED findings signal: measured values
are "F" (the review reported findings) and, by symmetry, a pass letter. Deriving
"did the reviewer find anything" from a regex over the transcript is a prose
heuristic, and the wrapper gates its authoritative vacuity check on that answer — so
the structured field must win wherever it exists.

`has_token_data` is reported rather than obeyed: a `false` there alongside readable
counts is itself a drift signal, and the counts are what the vacuity check needs.
"""

import json
import sys

INPUT_TOKEN_KEYS = ("input_tokens", "inputTokens", "prompt_tokens", "promptTokens")
CACHED_TOKEN_KEYS = (
    "cached_input_tokens",
    "cachedInputTokens",
    "cached_tokens",
    "cache_read_tokens",
    "cacheReadTokens",
)
OUTPUT_TOKEN_KEYS = (
    "total_output_tokens",
    "output_tokens",
    "outputTokens",
    "completion_tokens",
    "completionTokens",
)
TOKEN_CONTAINER_KEYS = ("token_usage", "tokenUsage", "usage", "token_counts")
# `source_machine_id` NAMES THE DAEMON THAT ISSUED THE JOB ID (#3654). Job ids are
# PER-DAEMON: two fleet boxes legitimately presented the same `job=265` for different
# reviews, and a coordination lead read the repetition as a collision and withheld a
# valid absence waiver. It is a STRING FACT like the others and nothing more — it is
# reported so the wrapper's block can NAME the daemon, never asserted on.
#
# MEASURED (roborev v0.61.2, this fleet): `roborev list --json` ROWS carry it at top
# level beside id/git_ref/branch/repo_path; `roborev show <id> --json` does NOT carry it
# ANYWHERE, not even in its nested `job` object. So its absence from a given payload is a
# REAL, expected state and must render as such rather than as an empty value.
#
# ADDING IT CANNOT CHANGE WHICH ROW `find_job` SELECTS: that function looks only at the
# id-bearing keys and then prefers a match carrying `git_ref`; STRING_FACTS is read solely
# by the output loop below, after the row has been chosen.
STRING_FACTS = ("git_ref", "status", "model", "requested_model", "verdict", "source_machine_id")


def objects(node):
    """Yield every dict in an arbitrarily nested JSON document."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            for found in objects(value):
                yield found
    elif isinstance(node, list):
        for value in node:
            for found in objects(value):
                yield found


def find_job(data, want):
    # `roborev show <id> --json` returns a REVIEW row whose top level carries
    # [agent, closed, created_at, id, job, job_id, output, prompt, uuid, verdict_bool]
    # and nests the JOB row — git_ref, status, model, requested_model, token_usage,
    # verdict — under a "job" key. Both objects answer to the same id, so returning the
    # FIRST id match handed back the review row, which has none of the fields the
    # asserts need. Prefer an id match that actually carries git_ref (measured, issue
    # #2964 round 6); fall back to the first match only if none does.
    matches = []
    for obj in objects(data):
        for key in ("id", "job_id", "job"):
            if key in obj and not isinstance(obj[key], (dict, list)) and str(obj[key]) == want:
                matches.append(obj)
                break
    for obj in matches:
        if "git_ref" in obj:
            return obj
    if matches:
        return matches[0]
    # A `show --json` payload may be the single job with no id echoed back. Accept that
    # ONLY when the payload IS one top-level object (codex, issue #2964 round 5): for a
    # list or a nested collection the first object carrying git_ref/token data can be an
    # UNRELATED or EARLIER job, and a previous review of the same range would then
    # falsely certify the job we just enqueued.
    if isinstance(data, dict) and (
        "git_ref" in data or any(key in data for key in TOKEN_CONTAINER_KEYS)
    ):
        return data
    return None


def token_container(job):
    """Return (raw_present, mapping) for the job's token accounting."""
    for key in TOKEN_CONTAINER_KEYS:
        if key not in job:
            continue
        raw = job[key]
        if isinstance(raw, str):
            # The real payload double-encodes this. Decode again.
            try:
                raw = json.loads(raw)
            except ValueError:
                return True, None
        if isinstance(raw, dict):
            return True, raw
        return True, None
    # Some builds may inline the counts on the job itself.
    if any(key in job for key in INPUT_TOKEN_KEYS + OUTPUT_TOKEN_KEYS):
        return True, job
    return False, None


def as_int(mapping, keys):
    for key in keys:
        if key not in mapping:
            continue
        value = mapping[key]
        if isinstance(value, bool) or value is None:
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            text = value.strip()
            if text.lstrip("-").isdigit():
                return int(text)
    return None


def main(argv):
    if len(argv) not in (4, 5):
        sys.stderr.write(
            "usage: roborev-job-facts.py <job-id> <facts-out> <prompt-out> [<review-output-out>]\n")
        return 2
    want, facts_path, prompt_path = argv[1], argv[2], argv[3]
    # THE OPTIONAL FOURTH PATH IS FOR RECHECK MODE (#3312 job 24): re-evaluating a completed job has no
    # transcript of its own, so `review-completed`, the vacuity tiers and `findings` must be re-asserted
    # against the review text THE RECORD carries. Writing it out here keeps ONE implementation of
    # "find this job in whichever payload shape roborev returned" rather than a second parse at the
    # call site. Absent or empty output leaves the file empty, which those checks read as a FAILURE to
    # establish a completed review — the fail-closed direction.
    output_path = argv[4] if len(argv) == 5 else None
    try:
        data = json.load(sys.stdin)
    except ValueError:
        return 1
    job = find_job(data, want)
    if job is None:
        return 1

    lines = []
    for key in STRING_FACTS:
        value = job.get(key)
        if isinstance(value, str) and value.strip():
            lines.append("%s=%s" % (key, " ".join(value.split())))
    if isinstance(job.get("has_token_data"), bool):
        lines.append("has_token_data=%s" % str(job["has_token_data"]).lower())

    present, usage = token_container(job)
    counts = {}
    if usage is not None:
        counts = {
            "input_tokens": as_int(usage, INPUT_TOKEN_KEYS),
            "cached_input_tokens": as_int(usage, CACHED_TOKEN_KEYS),
            "output_tokens": as_int(usage, OUTPUT_TOKEN_KEYS),
        }
    # `input` and `cached` are the two counts the vacuity check asserts on, so they
    # decide the state; `output` is advisory and may legitimately be missing.
    readable = counts.get("input_tokens") is not None and counts.get("cached_input_tokens") is not None
    if not present:
        state = "absent"
    elif readable:
        state = "parsed"
    else:
        state = "unparseable"
    lines.append("token_state=%s" % state)
    for key in ("input_tokens", "cached_input_tokens", "output_tokens"):
        value = counts.get(key)
        lines.append("%s=%s" % (key, "" if value is None else value))

    with open(facts_path, "w") as handle:
        handle.write("\n".join(lines) + "\n")

    prompt = job.get("prompt")
    if isinstance(prompt, str) and prompt.strip():
        with open(prompt_path, "w") as handle:
            handle.write(prompt)
    if output_path is not None:
        # The review text lives beside the job row on the REVIEW payload, and roborev exposes it under
        # EITHER name: `output` or `verdict_text`. BOTH are read, on the review object AND on the nested
        # job object, because documenting a field and then not reading it produced an empty transcript on
        # any payload that used the other spelling — which spuriously FAILed `review-completed` and
        # `findings` on a legitimate recheck (#3312 job 25). Written VERBATIM — no whitespace collapsing —
        # because the terminal-verdict-marker regex is line-anchored.
        review_out = None
        for container in (data if isinstance(data, dict) else {}, job):
            if not isinstance(container, dict):
                continue
            for key in ("output", "verdict_text"):
                candidate = container.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    review_out = candidate
                    break
            if review_out is not None:
                break
        with open(output_path, "w") as handle:
            handle.write(review_out if isinstance(review_out, str) else "")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
