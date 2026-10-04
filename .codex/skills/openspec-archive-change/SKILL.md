---
name: openspec-archive-change
description: Archive a completed change in the experimental workflow. Use when the user wants to finalize and archive a change after implementation is complete.
license: MIT
compatibility: Requires openspec CLI.
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.4.1"
---

## CQLite contract

Read repository-root `AGENTS.md` and `.agents/skills/cqlite-development/SKILL.md`.
Use available Codex tools or ordinary conversation; honor approval already given
in this session. Missing owner decisions block dependent implementation, not
independent preparation. This skill does not authorize external posting by itself.
The OpenSpec CLI's context/rules and resolved paths remain authoritative for artifacts.

Archive a completed change in the experimental workflow.

**Input**: Optionally specify a change name. If omitted, check if it can be inferred from conversation context. If vague or ambiguous you MUST prompt for available changes.

**CQLite delivery prerequisite:** Establish the linked PR and confirm its live
state is MERGED, the delivery is complete rather than a slice, and required gate,
intent audit, test-quality review and final review evidence apply to the merged
revision (or its certified PR head/squash mapping). Checkboxes and artifact status
alone are insufficient. With no merged PR or missing evidence, leave the change
active and report what remains; do not offer an incomplete-success override.
An explicitly requested cancellation is not successful feature completion.

**Steps**

1. **Resolve the requested change**

   Use a name explicitly supplied or unambiguously established in this conversation.
   Only if ambiguous, perform the selection below.

   Run `openspec list --json` to get available changes. Use the available user-input tool or ordinary conversation to let the user select.

   Show only active changes (not already archived).
   Include the schema used for each change if available.

   **IMPORTANT**: Do not guess between ambiguous changes; ask only when context does not resolve the choice.

2. **Check artifact completion status**

   Run `openspec status --change "<name>" --json` to check artifact completion.

   Parse the JSON to understand:
   - `schemaName`: The workflow being used
   - `planningHome`, `changeRoot`, `artifactPaths`, and `actionContext`: path and scope context
   - `artifacts`: List of artifacts with their status (`done` or other)

   If status reports `actionContext.mode: "workspace-planning"`, explain that workspace archive is not supported in this slice and STOP. Do not move workspace changes into repo-local archives or edit linked repos.

   **If any artifacts are not `done`:**
   - Display warning listing incomplete artifacts
   - Leave the change active and report the incomplete work; do not archive it as completed.

3. **Check task completion status**

   Read the tasks file (typically `tasks.md`) to check for incomplete tasks.

   Count tasks marked with `- [ ]` (incomplete) vs `- [x]` (complete).

   **If incomplete tasks found:**
   - Display warning showing count of incomplete tasks
   - Leave the change active and report the incomplete work; do not archive it as completed.

   **If no tasks file exists:** Establish completion from the schema and independent evidence; absence of a tasks file is not proof of completion.

4. **Assess delta spec sync state**

   Use `artifactPaths.specs.existingOutputPaths` from status JSON to check for delta specs. If none exist, proceed without sync prompt.

   **If delta specs exist:**
   - Compare each delta spec with its corresponding main spec at `openspec/specs/<capability>/spec.md`
   - Determine what changes would be applied (adds, modifications, removals, renames)
   - Show a combined summary before prompting

   **Prompt options:**
   - If changes are needed for completed delivery: sync them before archiving.
   - If already synced and delivery prerequisites hold: archive without repeating the sync.

   When sync is needed for a completed CQLite delivery, read `../openspec-sync-specs/SKILL.md` and perform the sync directly with available file tools. Verify the result before archiving. Do not invent a task or skill-invocation API. If sync cannot be completed, leave the change active and report the gap.

5. **Perform the archive**

   Create an `archive` directory under `planningHome.changesDir` if it doesn't exist:
   ```bash
   mkdir -p "<planningHome.changesDir>/archive"
   ```

   Generate target name using current date: `YYYY-MM-DD-<change-name>`

   **Check if target already exists:**
   - If yes: Fail with error, suggest renaming existing archive or using different date
   - If no: Move `changeRoot` to the archive directory

   ```bash
   mv "<changeRoot>" "<planningHome.changesDir>/archive/YYYY-MM-DD-<name>"
   ```

6. **Display summary**

   Show archive completion summary including:
   - Change name
   - Schema that was used
   - Archive location
   - Whether specs were synced (if applicable)
   - Note about any warnings (incomplete artifacts/tasks)

**Output On Success**

```
## Archive Complete

**Change:** <change-name>
**Schema:** <schema-name>
**Archived to:** the archive path derived from `planningHome.changesDir`/YYYY-MM-DD-<name>/
**Specs:** ✓ Synced to main specs (or "No delta specs")

All artifacts complete. All tasks complete.
```

**Guardrails**
- Use explicit or unambiguous conversation context; prompt only when the change is ambiguous.
- Use artifact graph (openspec status --json) for completion checking
- Incomplete requirements, missing verification, or unmerged work block completed-delivery archival.
- Preserve .openspec.yaml when moving to archive (it moves with the directory)
- Show clear summary of what happened
- If sync is requested, use openspec-sync-specs approach (agent-driven)
- If delta specs exist, always run the sync assessment and show the combined summary before prompting
