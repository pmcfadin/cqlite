---
title: "For Agents: Developing CQLite"
description: Contributor doctrine, gate contracts, and development workflows for AI agents working on CQLite itself.
sidebar:
  label: Overview
  order: 0
---

This section documents the contributor doctrine, gate contracts, and development
workflows for AI agents (and humans) working on CQLite itself.

Write for AI agents: terse, imperative, copy-pasteable commands. Skip the prose.

## Pages in this section

| Page | What it covers |
|------|----------------|
| [Gate contract](/cqlite/agents-developing/gate-contract/) | `scripts/agent-gate.sh` — the only run that counts; summary-block format |
| [No-heuristics mandate](/cqlite/agents-developing/no-heuristics/) | Authoritative metadata only; legacy fallbacks behind flags (issue #28) |
| [Test data](/cqlite/agents-developing/test-data/) | Fetching datasets, dataset pins, CQLITE_DATASETS_ROOT, missing-data behaviour |
| [Key source paths](/cqlite/agents-developing/source-map/) | Where parsers, writers, query engine, and bindings live |
| [sstabledump validation playbook](/cqlite/agents-developing/validation-playbook/) | JSONL golden files, parity tests, smoke-test-all-tables |
| [Format debugging workflow](/cqlite/agents-developing/format-debugging/) | Hex dumps, definitive-guide chapters, appendix F known limitations |

## Non-negotiable rules

1. Run `scripts/agent-gate.sh` before opening any PR. Paste its summary block verbatim. Ad-hoc `cargo test` runs do not count.
2. Use authoritative metadata only — no type guessing, no heuristics (see [no-heuristics mandate](/cqlite/agents-developing/no-heuristics/)).
3. Integration tests use real SSTable data. Fetch it before running: `bash test-data/scripts/fetch-datasets.sh`.
4. `RUSTFLAGS="-D warnings"` must pass — zero clippy warnings allowed.

## Quick-start for a new agent

```bash
# 1. Fetch test data
bash test-data/scripts/fetch-datasets.sh

# 2. Run the gate
scripts/agent-gate.sh

# 3. Paste the AGENT-GATE SUMMARY block in your PR report
```
