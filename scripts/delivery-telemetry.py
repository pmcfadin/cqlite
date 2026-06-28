#!/usr/bin/env python3
"""delivery-telemetry.py — delivery-pipeline telemetry ledger + recurring retro.

Closes the pipeline self-improvement loop:
  sense    -> an append-only ledger, one record per completed issue (`record`)
  diagnose -> rank recorded failures, file a deduped flow-meta issue (`retro`)
  improve  -> that issue runs through the normal pipeline

Authoritative-data-only mandate (CLAUDE.md / issue #28): every field is an observed
event — a GitHub timestamp/label or a run counter supplied by the stamping step — or
arithmetic over authoritative timestamps. Nothing is inferred, estimated, or guessed.
A required run counter that was not observed is an ERROR, never a fabricated zero.

Subcommands:
  record    build one ledger record and append it (one line)
  lint      schema-validate every line of the ledger        (alias: validate)
  retro     rank failure categories; optionally file a deduped flow-meta issue

The two GitHub-touching paths (`record` live-pull, `retro --file`) sit behind explicit
seams (`--from-json`, `--open-issues-json`, `--ledger`) so the unit tests run with no
network and no datasets.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------- paths

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = REPO_ROOT / "docs" / "reports" / "delivery-telemetry.jsonl"
DEFAULT_SCHEMA = REPO_ROOT / "docs" / "reports" / "delivery-telemetry.schema.json"

# --------------------------------------------------------------------------- retro policy
#
# Failure-category weights are POLICY CONSTANTS (a documented, deterministic tally —
# not an inferred or learned cost model). The retro rank is Sum(count * weight) per
# category over the ledger. Tuning a weight is a constant edit, never a guess about
# what the data means. Order of magnitude reflects how expensive each failure is to
# the pipeline: a failed gate / rework round costs the most; a single rebase the least.
RETRO_WEIGHTS = {
    "gate_failures": 5,      # an agent-gate.sh FAIL round
    "rework": 4,             # a re-open / re-review round
    "claim_collisions": 3,   # a lost claim race (wasted setup)
    "roborev_findings": 2,   # a roborev finding to resolve
    "rebase_events": 2,      # a rebase / conflict resolution
}

# Required run counters for `record` — each MUST be supplied; a missing one is an error
# (authoritative-data-only: we never fabricate a count we did not observe).
REQUIRED_COUNTERS = ("claim_collisions", "rebase_events", "gate_runs", "roborev_findings", "rework")


# ============================================================ minimal JSON-Schema check
#
# A pure-stdlib subset validator (keeps the tool dependency-free / SKIP-aware on python3
# alone). The schema file remains the source of truth; this enforces the keywords the
# record schema actually uses: type, required, properties, const, enum, minimum, pattern.

def _type_ok(value, t: str) -> bool:
    if t == "object":
        return isinstance(value, dict)
    if t == "array":
        return isinstance(value, list)
    if t == "string":
        return isinstance(value, str)
    if t == "integer":
        # bool is a subclass of int — exclude it
        return isinstance(value, int) and not isinstance(value, bool)
    if t == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if t == "boolean":
        return isinstance(value, bool)
    if t == "null":
        return value is None
    return True


def _validate(value, schema: dict, path: str, errors: list) -> None:
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: must equal {schema['const']!r}, got {value!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: must be one of {schema['enum']}, got {value!r}")
    if "type" in schema and not _type_ok(value, schema["type"]):
        errors.append(f"{path}: expected type {schema['type']}, got {type(value).__name__}")
        return  # further checks assume the type held
    if "minimum" in schema and isinstance(value, (int, float)) and not isinstance(value, bool):
        if value < schema["minimum"]:
            errors.append(f"{path}: {value} < minimum {schema['minimum']}")
    if "pattern" in schema and isinstance(value, str):
        if not re.search(schema["pattern"], value):
            errors.append(f"{path}: {value!r} does not match /{schema['pattern']}/")
    if schema.get("format") == "date-time" and isinstance(value, str):
        try:
            parsed = _parse_ts(value)
        except ValueError:
            errors.append(f"{path}: {value!r} is not a valid RFC-3339 date-time")
        else:
            # fromisoformat accepts date-only / tz-naive strings on 3.11+; require a full
            # date + time + offset so a value like "2026-06-01" is rejected.
            if "T" not in value or parsed.tzinfo is None:
                errors.append(f"{path}: {value!r} is not a full date-time with offset")
    if schema.get("type") == "object" and isinstance(value, dict):
        props = schema.get("properties", {})
        for req in schema.get("required", []):
            if req not in value:
                errors.append(f"{path}: missing required field '{req}'")
        # additionalProperties: false -> a typo'd/extra key (e.g. 'reworkk') is an error,
        # not silently ignored (keeps the schema the source of truth).
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in props:
                    errors.append(f"{path}: unknown field '{key}' (not in schema)")
        for key, subschema in props.items():
            if key in value:
                _validate(value[key], subschema, f"{path}.{key}" if path else key, errors)


def validate_record(record: dict, schema: dict) -> list:
    """Return a list of human-readable validation errors (empty == valid)."""
    errors: list = []
    _validate(record, schema, "", errors)
    return errors


def load_schema(schema_path: Path) -> dict:
    return json.loads(schema_path.read_text())


# ============================================================ timestamp helpers

def _parse_ts(value: str) -> datetime:
    """Parse an RFC-3339 / ISO-8601 UTC timestamp (accepts a trailing 'Z')."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _seconds_between(start: str, end: str) -> int:
    return int((_parse_ts(end) - _parse_ts(start)).total_seconds())


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================================================ record

def _github_fields(issue: int, pr: int) -> dict:
    """Pull authoritative timestamps/labels live from `gh` (only when not injected)."""
    issue_json = json.loads(subprocess.run(
        ["gh", "issue", "view", str(issue), "--json", "createdAt,closedAt,labels"],
        check=True, capture_output=True, text=True).stdout)
    pr_json = json.loads(subprocess.run(
        ["gh", "pr", "view", str(pr), "--json", "createdAt,mergedAt"],
        check=True, capture_output=True, text=True).stdout)
    labels = [l["name"] for l in issue_json.get("labels", [])]
    priority = next((l for l in labels if re.fullmatch(r"P[0-3]", l)), None)
    # Routing is authoritative, not inferred: set it ONLY from an explicit label. If
    # neither is present, leave it None so build_record requires --routing rather than
    # silently guessing "design".
    if "oracle" in labels:
        routing = "oracle"
    elif "design" in labels:
        routing = "design"
    else:
        routing = None
    return {
        "created_at": issue_json["createdAt"],
        "closed_at": issue_json["closedAt"],
        "pr_opened_at": pr_json["createdAt"],
        "merged_at": pr_json["mergedAt"],
        "priority": priority,
        "routing": routing,
    }


def build_record(args, gh_fields: dict) -> dict:
    """Assemble a record from supplied counters + authoritative GitHub fields."""
    for counter in REQUIRED_COUNTERS:
        if getattr(args, counter) is None:
            raise SystemExit(
                f"error: --{counter.replace('_', '-')} is required "
                f"(authoritative-data-only: a counter that was not observed is never defaulted)")
    if args.gate is None:
        raise SystemExit("error: --gate {pass|fail} is required")

    # Authoritative timestamps must all be present — a finalize runs only on a merged,
    # closed issue. A null here (e.g. an unmerged PR) is an error, not silent arithmetic
    # on None (which would otherwise raise an opaque AttributeError).
    for key in ("created_at", "pr_opened_at", "merged_at", "closed_at"):
        if not gh_fields.get(key):
            raise SystemExit(
                f"error: authoritative timestamp '{key}' is missing/null "
                f"(finalize records only a merged, closed issue)")

    created = gh_fields["created_at"]
    pr_opened = gh_fields["pr_opened_at"]
    merged = gh_fields["merged_at"]
    closed = gh_fields["closed_at"]
    priority = args.priority or gh_fields.get("priority")
    routing = args.routing or gh_fields.get("routing")
    if priority is None:
        raise SystemExit("error: priority not found on the issue and not supplied via --priority")
    if routing is None:
        raise SystemExit("error: routing not determinable from labels and not supplied via "
                         "--routing {design|oracle} (authoritative-data-only: never inferred)")

    return {
        "schema": 1,
        "issue": args.issue,
        "slug": args.slug,
        "pr": args.pr,
        "routing": routing,
        "priority": priority,
        "created_at": created,
        "pr_opened_at": pr_opened,
        "merged_at": merged,
        "closed_at": closed,
        "cycle_time_s": _seconds_between(created, closed),
        "phase_s": {
            "to_pr_s": _seconds_between(created, pr_opened),
            "review_s": _seconds_between(pr_opened, merged),
        },
        "claim_collisions": args.claim_collisions,
        "rebase_events": args.rebase_events,
        "gate": args.gate,
        "gate_runs": args.gate_runs,
        "roborev_findings": args.roborev_findings,
        "rework": args.rework,
        "stamped_at": _now_iso(),
    }


def cmd_record(args) -> int:
    schema = load_schema(Path(args.schema))
    if args.from_json:
        gh_fields = json.loads(Path(args.from_json).read_text())
    else:
        gh_fields = _github_fields(args.issue, args.pr)

    record = build_record(args, gh_fields)
    errors = validate_record(record, schema)
    if errors:
        print("error: built record is not schema-valid:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    ledger = Path(args.ledger)
    # Idempotency: one record per completed issue. A re-run / double finalize must not
    # append a second record (which would double-count that issue in retro). Refuse
    # unless --allow-duplicate is given.
    if ledger.exists():
        for raw in ledger.read_text().splitlines():
            if not raw.strip():
                continue
            try:
                existing = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if existing.get("issue") == record["issue"] and not args.allow_duplicate:
                print(f"error: issue #{record['issue']} already has a ledger record "
                      f"(pass --allow-duplicate to override)", file=sys.stderr)
                return 1

    ledger.parent.mkdir(parents=True, exist_ok=True)
    with ledger.open("a") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")
    print(f"recorded issue #{record['issue']} (pr #{record['pr']}) -> {ledger}")
    return 0


# ============================================================ lint

def cmd_lint(args) -> int:
    schema = load_schema(Path(args.schema))
    ledger = Path(args.ledger)
    if not ledger.exists():
        print(f"error: ledger not found: {ledger}", file=sys.stderr)
        return 1

    bad = 0
    seen_issues: dict = {}
    for lineno, raw in enumerate(ledger.read_text().splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            print(f"line {lineno}: invalid JSON: {exc}", file=sys.stderr)
            bad += 1
            continue
        errors = validate_record(record, schema)
        if errors:
            bad += 1
            for e in errors:
                print(f"line {lineno}: {e}", file=sys.stderr)
        # one record per completed issue — a duplicate 'issue' skews retro
        issue = record.get("issue")
        if issue in seen_issues:
            bad += 1
            print(f"line {lineno}: duplicate record for issue #{issue} "
                  f"(first seen line {seen_issues[issue]})", file=sys.stderr)
        elif issue is not None:
            seen_issues[issue] = lineno

    if bad:
        print(f"FAIL: {bad} malformed record(s)", file=sys.stderr)
        return 1
    print("OK: ledger is well-formed")
    return 0


# ============================================================ retro

def aggregate(records: list) -> dict:
    """Sum each failure category across records (authoritative recorded values only).

    Precondition: every record is already schema-valid (every counter present). Callers
    MUST validate first — `cmd_retro` does. Fields are indexed directly (not `.get` with a
    default) so an unvalidated record raises rather than contributing a fabricated zero,
    consistent with the authoritative-data-only mandate.
    """
    tally = {k: 0 for k in RETRO_WEIGHTS}
    for r in records:
        tally["claim_collisions"] += r["claim_collisions"]
        tally["rebase_events"] += r["rebase_events"]
        tally["roborev_findings"] += r["roborev_findings"]
        tally["rework"] += r["rework"]
        # Failed gate ROUNDS, derived from the authoritative run count: every run but
        # the final pass was a failure. A terminal-fail issue (gate == "fail") failed
        # every round. gate_runs >= 1 by schema. This counts a 3-run-then-pass issue as
        # 2 failed rounds, not 0 (matching the weight's "an agent-gate.sh FAIL round").
        passed_final = 1 if r["gate"] == "pass" else 0
        tally["gate_failures"] += max(0, r["gate_runs"] - passed_final)
    return tally


def rank(tally: dict) -> list:
    """Return [(category, count, weight, score)] sorted by score desc, then category."""
    rows = [(cat, cnt, RETRO_WEIGHTS[cat], cnt * RETRO_WEIGHTS[cat]) for cat, cnt in tally.items()]
    rows.sort(key=lambda r: (-r[3], r[0]))
    return rows


def _retro_marker(category: str) -> str:
    return f"<!-- RETRO:{category} -->"


_FLOW_META_LIMIT = 500


def _open_flow_meta_issues(args) -> list:
    if args.open_issues_json:
        return json.loads(Path(args.open_issues_json).read_text())
    issues = json.loads(subprocess.run(
        ["gh", "issue", "list", "--label", "flow-meta", "--state", "open",
         "--json", "number,title,body", "--limit", str(_FLOW_META_LIMIT)],
        check=True, capture_output=True, text=True).stdout)
    # No silent cap: if the lookup hit the limit, dedupe may be incomplete — say so loudly.
    if len(issues) >= _FLOW_META_LIMIT:
        print(f"warning: open flow-meta lookup hit the {_FLOW_META_LIMIT}-issue cap — "
              f"dedupe may miss an existing tracker beyond it", file=sys.stderr)
    return issues


def cmd_retro(args) -> int:
    ledger = Path(args.ledger)
    if not ledger.exists():
        print(f"error: ledger not found: {ledger}", file=sys.stderr)
        return 1
    # Refuse to rank a non-conforming ledger: a malformed/partial line or a record
    # missing a counter must be a clean error (run `lint`), never silently parsed or
    # defaulted — the same authoritative-data-only bar `record` enforces.
    schema = load_schema(Path(args.schema))
    records = []
    for lineno, raw in enumerate(ledger.read_text().splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            rec = json.loads(raw)
        except json.JSONDecodeError:
            print(f"error: ledger malformed at line {lineno} — run `lint`", file=sys.stderr)
            return 1
        if validate_record(rec, schema):
            print(f"error: ledger record at line {lineno} is not schema-valid — run `lint`",
                  file=sys.stderr)
            return 1
        records.append(rec)
    if not records:
        print("ledger is empty — nothing to retro")
        return 0

    tally = aggregate(records)
    ranked = rank(tally)

    print(f"delivery-pipeline retro over {len(records)} record(s):")
    print(f"  {'category':<18} {'count':>6} {'weight':>7} {'score':>7}")
    for cat, cnt, weight, score in ranked:
        print(f"  {cat:<18} {cnt:>6} {weight:>7} {score:>7}")

    top_cat, top_cnt, _, top_score = ranked[0]
    if top_score == 0:
        print("\nno recurring failures recorded — nothing to file")
        return 0
    print(f"\ntop recurring failure: {top_cat} (count={top_cnt}, score={top_score})")

    # dedupe against open flow-meta issues by stable category marker
    marker = _retro_marker(top_cat)
    existing = [i for i in _open_flow_meta_issues(args) if marker in (i.get("body") or "")]
    if existing:
        nums = ", ".join(f"#{i['number']}" for i in existing)
        print(f"already tracked by open flow-meta issue(s): {nums} — skipping filing")
        return 0

    title = f"flow-meta: reduce recurring '{top_cat}' (retro top failure)"
    body = (
        f"{marker}\n\n"
        f"## Retro finding\n"
        f"The recurring-retro over `{ledger}` ranks **{top_cat}** as the highest-cost "
        f"recurring pipeline failure (count={top_cnt}, weighted score={top_score}).\n\n"
        f"## Ranked categories\n"
        + "\n".join(f"- {c}: count={n}, score={s}" for c, n, _, s in ranked)
        + "\n\n_Filed by `scripts/delivery-telemetry.py retro`._\n"
    )

    if not args.file:
        print("\n--- DRY RUN (pass --file to create) ---")
        print(f"title: {title}")
        print(body)
        return 0

    out = subprocess.run(
        ["gh", "issue", "create", "--title", title, "--body", body,
         "--label", "flow-meta", "--label", "P2"],
        check=True, capture_output=True, text=True)
    print(f"filed: {out.stdout.strip()}")
    return 0


# ============================================================ argparse

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="retro weight table (policy constants): "
               + ", ".join(f"{k}={v}" for k, v in RETRO_WEIGHTS.items()),
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--ledger", default=str(DEFAULT_LEDGER), help="ledger path (JSONL)")
        sp.add_argument("--schema", default=str(DEFAULT_SCHEMA), help="JSON Schema path")

    rec = sub.add_parser("record", help="append one ledger record")
    common(rec)
    rec.add_argument("--issue", type=int, required=True)
    rec.add_argument("--pr", type=int, required=True)
    rec.add_argument("--slug", required=True)
    rec.add_argument("--routing", choices=["design", "oracle"], default=None)
    rec.add_argument("--priority", default=None, help="P0..P3 (else read from the issue)")
    rec.add_argument("--gate", choices=["pass", "fail"], default=None)
    rec.add_argument("--gate-runs", dest="gate_runs", type=int, default=None)
    rec.add_argument("--claim-collisions", dest="claim_collisions", type=int, default=None)
    rec.add_argument("--rebase-events", dest="rebase_events", type=int, default=None)
    rec.add_argument("--roborev-findings", dest="roborev_findings", type=int, default=None)
    rec.add_argument("--rework", type=int, default=None)
    rec.add_argument("--from-json", dest="from_json", default=None,
                     help="inject GitHub-derived fields from a JSON file (else pull via gh)")
    rec.add_argument("--allow-duplicate", dest="allow_duplicate", action="store_true",
                     help="append even if the issue already has a record (default: refuse)")
    rec.set_defaults(func=cmd_record)

    lint = sub.add_parser("lint", aliases=["validate"], help="schema-validate the ledger")
    common(lint)
    lint.set_defaults(func=cmd_lint)

    ret = sub.add_parser("retro", help="rank failures; optionally file a deduped issue")
    common(ret)
    ret.add_argument("--file", action="store_true", help="file a flow-meta issue (default: dry-run)")
    ret.add_argument("--open-issues-json", dest="open_issues_json", default=None,
                     help="inject open flow-meta issues from a JSON file (else query gh)")
    ret.set_defaults(func=cmd_retro)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
