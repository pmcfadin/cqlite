---
name: flow-groom
description: Turn a rough idea into one scoped, labeled GitHub issue (one P0–P3 + status:ready, testable acceptance criteria), and decide oracle-vs-design routing. First stage of the CQLite delivery pipeline. Use when the owner says "groom <idea>", "file an issue for X", or brings a rough idea to scope.
---

# flow-groom — idea → one scoped issue

You are the CQLite delivery lead. Turn a rough idea into exactly ONE well-scoped GitHub issue.

## Steps

1. **Clarify just enough.** Ask at most one question (one at a time) only if scope is genuinely
   ambiguous; otherwise pick a sensible scope and state it.
2. **Decide oracle vs design** (record it in the issue body):
   - **Oracle-driven** (SSTable parsing/decode, compaction/tombstone byte-parity, type system) — has a
     Cassandra/sstabledump source of truth. These get an issue + a pinned parity test and **skip
     OpenSpec**; they can go straight to `flow-implement`.
   - **Design-driven** (bindings/M6, query-engine surface, CLI/REPL UX, perf/M7, process) — real
     latitude, no oracle. These go through `flow-activate` (OpenSpec).
3. **Write testable acceptance criteria.** Each criterion must map to a check (a test or an sstabledump
   parity comparison). Note any no-heuristics / public-surface (wiring-evidence) / memory-budget impact.
4. **Create the issue** with exactly one priority label and `status:ready`:
   ```bash
   gh issue create --title "<concise>" --body "<context + oracle/design + acceptance criteria>" \
     --label "P2" --label "status:ready"
   ```
   (Pick P0–P3 deliberately; confirm priority with the owner if unsure.) The `status:*` labels are
   **decorative mirrors only** — board `Status` is the sole dispatch authority (Path A, #1886); a
   label never selects work.
5. **Verify the board add — do NOT trust Project auto-add.** Auto-add has been observed missing all
   new issues, so add the item explicitly and confirm it landed:
   ```bash
   gh project item-add 1 --owner pmcfadin --url <issue-url>
   gh project item-edit --project-id <id> --id <item-id> --field-id <status-field> \
     --single-select-option-id <Ready-or-Backlog>   # Ready if groomed-ready, else Backlog
   gh project item-list 1 --owner pmcfadin --limit 1000 | grep "<issue-#>"   # confirm item + Status
   ```
   The item MUST appear with the intended `Status` before you report done.
6. **Report** the issue number + a one-line description (`#<N> (<slug>)`) and whether it's oracle- or
   design-driven (i.e. whether `flow-activate` or a direct `flow-implement` is next).

Do not create worktrees or specs here — that's `flow-activate`. One idea → one issue.
