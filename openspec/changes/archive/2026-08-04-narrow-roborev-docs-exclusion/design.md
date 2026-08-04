# Design: narrow the `docs/` review exclusion (issue #3229)

## Context

The delivery pipeline treats "roborev clean" as a merge condition. `.roborev.toml` set
`exclude_patterns = ['docs/**', '*.md']`, which discarded every path under `docs/` from the diff roborev
constructs — including the measurement harnesses the repo ships **by convention** under
`docs/reports/*-artifacts/`. On PR #3222 that produced an empty prompt for a 136-file code census; the
#2964 wrapper FAILed the round, so nothing unreviewed merged, but the class of PR became
non-certifiable. This design records how the exclusion is narrowed — and, after the owner's descope
ruling, why the wrapper is deliberately **not** given a mechanism that predicts the exclusion set.

**Scope after the descope ruling: AC1, AC2, AC5, AC6, AC7 ship. AC3 and AC4 are DEFERRED to #3283.**
roborev's compiled-in built-in deny-list — a separate, still-open, still-unmodelled thing — is **#3278**.

## D0 — THE DESCOPE RULING (owner, 2026-08-04): the exclusion oracle is DELETED, deferred to #3283

### What was removed

A pre-enqueue oracle in `scripts/flow/roborev-review-oracles.sh` that **predicted roborev's effective
exclusion set** and reported under its own summary key. Its parts, all deleted:

- a **bash port of roborev's Go `git.FormatExcludeArgs`** (pathspec construction);
- a **TOML array parser** for `exclude_patterns`, including quoted-key spellings and basic-string escapes;
- a **three-source config union** — the worktree `.roborev.toml`, the ROOT checkout's `.roborev.toml`, and
  the global `~/.roborev/config.toml`;
- a **`roborev config get` corroboration oracle** with an OK / DRIFT / NOTICE / UNAVAILABLE grammar;
- a **trailing-slash FAIL** (the `docs/` ⇄ `docs/**` inversion);
- the **survivor computation** (`git diff --name-only -z --no-renames <base>...HEAD -- <pathspecs>` and
  `swallowed = census CODE paths − survivors`);
- the summary key it reported under, its value grammar, its state variable, its call site, its
  verdict-scan registration and its `--help` documentation;
- every regression case family that exercised it, and the fixtures' ability to write a `.roborev.toml`.

### Why — the ruling's reasoning, recorded because a deletion needs one

**The false-PASS trajectory was going the wrong way.** Review found false-PASS blockers *inside that
oracle* in four consecutive rounds, at an **INCREASING** rate:

| round | false-PASS blockers found inside the oracle |
|---|---|
| 8 | 1 |
| 9 | 1 |
| 10 | 2 |
| 11 | **3** |

And the decisive detail: **two of round 11's three defects lived in code the two preceding fix rounds had
just introduced.** That is not a subsystem converging. It is a surface where the fixes add defects of the
class they close, so each round buys a smaller expected reduction in risk than it costs.

**A guard with known documented false-PASSes is worse than no guard, because it invites reliance it cannot
support.** A documented false-PASS is not a caveat a reader applies at the moment it matters; it is a
footnote under a green line. The guard's whole value was that an agent could stop thinking about the
exclusion set — which is exactly the reliance a known-holey guard cannot carry.

**Subtraction cannot add a false-PASS.** With nothing modelling roborev's exclusions, no key can excuse a
path from `prompt-content:`, no verdict rests on a prediction, and every failure direction the deletion
opens is a FAIL. The wrapper now learns about a swallow the expensive way — from `prompt-content:`, after
the review round rather than before it — and that is a *diagnostic* loss, not a correctness one.

**Three known-but-unfixed defects are removed WITH their subject, not fixed:**

1. a **corroboration gate** that could reach a positive verdict without an affirmative measurement;
2. an **NBSP `TrimSpace` port divergence** — Go's `unicode.IsSpace` trims U+00A0, bash trims do not, so a
   pattern carrying a non-breaking space resolved differently in the port than in roborev;
3. **prefix-glob holes** in the value/verdict matching around the key.

Nothing here was excused, waived, or judged acceptable-with-the-defect-present: the code the findings
described is gone, and what it would have guarded is recorded as deferred work under **#3283**.

### The class-level root cause, recorded for #3283

**A port is a second implementation, and a second implementation's correctness is only knowable by
differential testing against the original.** The oracle re-derived Go's trim rules in bash and was tested
against a *model* of Go, not against Go — which is why the NBSP divergence was **unfindable by care**. No
amount of reading `FormatExcludeArgs` more attentively produces `unicode.IsSpace`'s table. #3283 must
therefore either (a) obtain the resolved pathspecs from roborev itself, or (b) stand up a differential
harness that feeds the same pattern corpus to the real binary and to the port and compares, before any
verdict rests on the port. A port with no differential oracle is not a guard; it is a second guess with a
green line over it.

### What this costs, stated plainly

**This IS a reduction in coverage, and it is an acceptable one.** The absent coverage, named exactly:
*there is no automated guard against a future `.roborev.toml` re-broadening; the regression it would catch
is a hand edit to a version-controlled file on `main`, and AC6's doctrine names the hazard in prose.* The
compensating properties are that the narrowed value is committed and reviewable, that a re-broadening is a
visible diff to a file whose comment block says `docs/**` MUST NOT be reintroduced, and that
`prompt-content:` still fails closed on the consequence.

## RECORDED FOR #3283 — what is measured about `exclude_patterns`, and NO CODE IN THIS CHANGE RELIES ON IT

Everything in this section is primary-source measurement, kept because #3283 will need it. It is **not** a
description of anything this change implements: the port and the checks it specified are deleted (D0).
Read it as a research record.

**The caveat that governs all of it: reading the instructions is not the same as reproducing them.** The
removed port had this whole section available to it and still diverged from Go on U+00A0. Fidelity to a
disassembly is necessary and nowhere near sufficient.

### The observational phase

`roborev` is an **external, stripped Go binary** — `roborev v0.61.2` at `/usr/local/bin/roborev`, no
source available — so its behaviour was established by symbol inspection, an empirical replay, and finally
by disassembling the responsible function.

1. **It is git pathspec, not a bespoke matcher.** The binary carries the symbols
   `git.FormatExcludeArgs` and `config.ResolveExcludePatterns`, and both string constants
   `:(exclude,glob)` and `:(exclude,glob)**/` are present. So the semantics are git wildmatch with
   `WM_PATHNAME`: anchored at the repository root, and `*` does not cross `/`.
2. **A slash-less pattern is applied recursively.** Replaying **21 real reviews** recorded in
   `~/.roborev/reviews.db` against their own `git_ref` ranges and diffing the census against the prompt
   actually sent, the only paths ever dropped were **25 paths, every one a `.md`** — including
   `.claude/agents/*.md`, `openspec/**/*.md`, `website/**/*.md` and top-level `CLAUDE.md`. `docs/**`
   cannot explain any of those, so `*.md` must be normalised to `**/*.md`. Corroborated locally:
   `git ls-files -- ':(exclude,glob)*.md'` leaves 1393 `.md` files (only the 11 top-level ones are
   excluded) whereas `':(exclude,glob)**/*.md'` leaves 0.
3. **Nothing but `.md` has ever been excluded.** In the same replay every non-`.md` file reached its
   prompt — `.github/workflows/*.yml`, `scripts/*.sh`, `scripts/flow/*.py`, `.rs`. Combined with (2),
   **`*.md` alone already excludes all ~1404 tracked `.md` files repo-wide**, and `docs/**` was the *sole*
   cause of executables under `docs/` being discarded. **This is the finding AC1 rests on**, and it is the
   one part of this section that remains load-bearing for shipped work.

### The exact algorithm, recovered by disassembly

`git.FormatExcludeArgs` was recovered from the stripped binary by parsing `.gopclntab` for symbols (real
text base `0x401000`) and disassembling it. It is eight lines:

```go
p = strings.TrimSpace(pattern)
p = strings.TrimRight(p, "/")
if p == "" { continue }
b0 = p[0]                       // read BEFORE TrimLeft
p = strings.TrimLeft(p, "/")
if p == "" { continue }
if b0 == '/' || strings.Index(p, "/") >= 0 {
    prefix = ":(exclude,glob)"       // verbatim, ROOT-ANCHORED
} else {
    prefix = ":(exclude,glob)**/"    // RECURSIVE
}
out = append(out, prefix+p, prefix+p+"/**")   // TWO pathspecs per pattern
```

It is on the **real diff path**, not a dead helper: callers are `git.GetDiffCtx`, `GetDiffLimitedCtx`,
`GetRangeDiffCtx`, `GetRangeDiffLimitedCtx`, `GetDirtyDiff`, and
`prompt.(*Builder).buildSinglePrompt` / `buildRangePrompt` / `resolveExcludes`.

**R1 — a slash-containing pattern is VERBATIM and ROOT-ANCHORED.** `docs/**/*.json` does **not** match
`website/src/content/docs/c.json`. This is what makes D1's docs-scoped deny-list exactly scoped: it hides
nothing elsewhere in the tree.

**R2 — every pattern silently emits TWO pathspecs**, `prefix+p` and `prefix+p+"/**"`. That is how a bare
directory name excludes its whole subtree, and any port that emits only the first would under-count
exclusions.

**R3 — the trailing-slash INVERSION, and it is the real trap.** `TrimRight(p, "/")` runs *before* the
contains-`/` test, so `docs/` → `docs` → slash-less → `**/docs` + `**/docs/**` = **RECURSIVE**, while
`docs/**` keeps its slash and stays root-anchored. **`docs/` and `docs/**` behave OPPOSITELY**, and the
trailing-slash form would also swallow `website/src/content/docs/**`. A trailing slash is a *silent
widening* of unbounded depth that reads like a harmless tidy-up. This change handles it as a **documented
configuration rule** (D1 and `.roborev.toml`'s comment: never write a trailing slash) — the automated
trailing-slash FAIL that once enforced it is deleted with the rest of the oracle and belongs to #3283.

**R4 — a leading `/` root-anchors an otherwise-recursive slash-less name.** `/README.md` →
`:(exclude,glob)README.md` (root only) vs `README.md` → `:(exclude,glob)**/README.md` (any depth). It is
the ONLY way to root-anchor a slash-less name, and it is why `b0` is captured *before* `TrimLeft`.

**R5 — no negation / re-include capability, VERIFIED at the instruction level.** `FormatExcludeArgs`
performs only TrimSpace / TrimRight / TrimLeft / `Index`; there is no `!` handling and no re-include path.
This upgrades "an allow-list is not expressible" from a working assumption to a verified fact, which is why
D1's deny-list was *forced*, not preferred. Empty-after-trim patterns are silently skipped.

**R6 — the model closes against the observations.** The pre-change `['docs/**', '*.md']` resolves to
root-anchored `docs/**` + `docs/**/**` and recursive `**/*.md` + `**/*.md/**`. That reproduces the
21-review replay *exactly*: only `.md` ever dropped, at arbitrary depth, repo-wide, and never a non-`.md`.

**R7 — the resolved set is not obtainable from the tool as it stands.** No roborev flag prints the resolved
pathspecs (`review --help` has no `--dry-run`; `-v` is global-only). `config.ResolveExcludePatterns` /
`loadRepoExcludePatterns` merge the global `~/.roborev/config.toml` with the repo `.roborev.toml` (global is
currently `[]`), i.e. a UNION. Two adjacent mechanisms must not be conflated with `exclude_patterns`:
`max_prompt_size`, `exclude_branches` and commit-message exclusion (`IsCommitMessageExcluded`) are separate
keys, and `git.EnsureLocalExcludePattern` / `infoExcludePath` writes `.git/info/exclude` — a different
mechanism entirely. **For #3283 this is the pivotal constraint**: R7 is *why* the removed design ported the
function instead of querying it, and it is therefore the first thing #3283 should try to change (an upstream
`--print-excludes` would retire the port and its whole defect class).

### The root-path / daemon-snapshot ordering, and the cross-worker arbitration it settled

roborev's daemon binds a repository by its **`repos.root_path`** — the ROOT checkout — and reads THAT
checkout's config, **snapshotting it at daemon start**. Under 1:1:1:1 every issue runs in a linked
worktree, so a worktree's `.roborev.toml` edit is **invisible** to the daemon until it is merged to the root
checkout *and* the daemon is restarted. Measured on this branch while the oracle still existed: the
worktree config predicted 7 surviving census paths, the root checkout's pre-change blanket set predicted 6,
and the real review delivered exactly those 6.

This closed the **live existential risk to this change**. Issue **#3234** had independently measured that
`exclude_patterns` has **"no observable effect"** — a null result which, if true, would have made AC1
cosmetic. The owner had ranked hypothesis **H2**: *config resolves from the primary checkout, not the
worktree.* Both halves are operative, found from opposite directions:

| Half | Established by |
|---|---|
| The **mechanism** — `exclude_patterns` really is applied, as `FormatExcludeArgs` pathspecs | this change: the disassembly + the 21-review replay |
| The **ordering** — the daemon reads the ROOT path's config and snapshots it at start | #3234, whose single daemon restart happened to precede every config edit it made and never follow one; and this change, from the other side |

**Conclusion, recorded plainly: `exclude_patterns` WORKS.** #3234's null result was a **worktree-config
artifact**, not a broken mechanism, so AC1 is a genuine fix. Two workers reached the same property from
opposite ends, which is stronger evidence than either alone.

**The generalisable doctrine, which survives the deletion:** (1) roborev's daemon reads `exclude_patterns`
from the repo ROOT PATH, so a worktree edit is invisible to it; (2) the daemon SNAPSHOTS config at start, so
an edit needs a RESTART; (3) generalised — **any PR whose subject is a config the daemon (or a gate) reads
from root cannot certify itself.** All three are in CLAUDE.md and `roborev-findings.md`.

### What lives under `docs/`, measured

1103 tracked files under `docs/`, by extension: md 356, txt 272, json 135, err 66, log 53, jsonl 46,
**sh 32**, png 23, svg 22, **py 22**, gz 18, pdf 10, mmd 10, html 9, yml 4, yaml 3, jfr 3, csv 3, c 3,
**bt 3**, extensionless 3, cql 2, toml 1, tex 1, rs 1, diff 1. Of those, 578 are under
`docs/reports/*-artifacts/**`: txt 246, json 133, err 66, log 53, jsonl 45, sh 30, py 21, gz 18, svg 12,
md 7, csv 3, c 3, bt 3, extensionless 3, yaml 2, cql 2, toml 1, rs 1, diff 1.

**The three extensionless files, RE-MEASURED — the earlier claim here was WRONG.** This design previously
recorded them as "compiled binaries … correctly not code to review". `file(1)` says otherwise: `ws0-readbw`
(16,856 B) and `ws0-stream` (21,112 B) are ELF 64-bit executables, but
`docs/reports/ws0-3217-artifacts/partB-run/offcputime-bigmap` (13,953 B) is a **Python script, ASCII text
executable** — 379 lines of reviewable source. All three are mode **100755**. So the "correctly not code"
justification was doing double duty for a real script, and it was the justification a prefix-only
extensionless rule rested on. Corrected rule: an extensionless path under a prose prefix is CODE **iff git
records it executable at EITHER ENDPOINT of the census range** (see the classification requirement in the
delta spec, and the endpoint discussion below). Consequences, both measured
on the final tree: docs/ executables classified CODE went **46/49 → 49/49**, and the docs/ code-path total
**75 → 78**, the delta being exactly those three paths and nothing else.

The two ELF blobs are now CODE census paths as well. That costs no prompt budget — git renders a binary
blob as `Binary files … differ` — and it remains *satisfiable*, because git still emits the
`diff --git a/<p> b/<p>` header the check matches on. The residual, stated rather than hidden: if roborev's
constructed diff should omit binary paths entirely, a PR touching one would surface as a
`prompt-content:` FAIL naming it. That is the fail-closed direction and it is diagnosable from the named
path; a by-name refinement, if it is ever wanted, belongs to **#3260** rather than here, because the
alternative — inspecting file CONTENT to guess "binary, therefore not code" — is precisely the kind of
byte-pattern inference the no-heuristics mandate forbids, while the executable bit is recorded metadata.

Read that histogram as the budget: the ~570 raw-output files (`txt`/`json`/`err`/`log`/`jsonl`) are what
makes a blanket un-exclusion unaffordable, and the ~60 `sh`/`py`/`bt`/`c`/`rs`/`toml`/`cql`/`yml` files
are what the reviewer must see. Measured on the final tree: **71 `docs/` executables now reach the
reviewer, 0 markdown does, and nothing outside `docs/` is newly excluded.**

### WHICH ENDPOINT'S MODE — both, as a disjunction

**The census subject is the RANGE `<base>...HEAD`, so a path is a code path if it is an executable
ANYWHERE in that range.** Both endpoints belong to the reviewed change and neither outranks the other, so
the rule is a **logical OR over the HEAD tree and the BASE tree** — not "the HEAD mode, falling back to
BASE", which is what this design said for one round and what the code then did.

That ordering was wrong, and measurably so. An implementation that consults HEAD, then BASE, and stops at
the first endpoint that yields a record never reaches BASE for a path present in HEAD — so a **pure
`chmod -x`** (`100755`@BASE → `100644`@HEAD) classified NON-CODE. Reproduced on a two-commit fixture with a
still-executable control (the control read CODE, so the probe was sound). The consequence is a false PASS
rather than a false FAIL: the path leaves `census_code_paths`, so `prompt-content: PASS (n/n)` is true of
the paths it counted and **silent about the one it dropped** — while `chmod -x` plainly does not turn a
Python script into prose. Deletion was already right (absent from HEAD ⇒ BASE consulted), which is exactly
why the four then-existing tests passed: every one of their fixtures has the path at **one** endpoint, so
the range semantics were untested.

The four combinations and the single rule that covers them:

| combination | example | classification |
|---|---|---|
| present at both, exec at either | `chmod -x` **or** `chmod +x`, or exec at both | **CODE** |
| present at both, exec at neither | a prose `docs/NOTICE` edited | non-code |
| HEAD only (added) | a new harness executable | **CODE** iff added executable |
| BASE only (deleted) | an executable removed | **CODE** iff it WAS executable (fail-closed: the removal is a code change) |
| neither | unreachable for a real census path | non-executable, no error |

**Why the fix is a SHAPE change and not a moved `return`.** This was the third round on this PR where a fix
reintroduced a narrower instance of the class it closed (prefix-only → HEAD-first ordered scan → …), and
each time the code was correct for the cases someone had thought of. So the remedy is structural, and it
rests on three properties that make skipping an endpoint *unexpressible*: the endpoint list is produced
**complete** before the fold begins; the fold's body contains **no `return`, `break` or `continue`** and can
only OR into a monotone accumulator, so the function's sole `return` is after the loop; and the
per-endpoint lookup is a **range-blind** function that names no endpoint, so there is no "first"/"then" for
a reader or an editor to get wrong. A structural test asserts that shape independently of the loop's
spelling, and is itself controlled against an injected early exit and against the prior ordered-scan
implementation verbatim — a shape assert that cannot fire is not evidence.

Mutation results on the whole guard suite, both directions: restoring the ordered scan turns **19**
assertions RED; consulting only HEAD turns **19** RED (the deleted-executable and `chmod -x` paths read
NON-CODE); consulting only BASE turns **17** RED (the added-executable and `chmod +x` paths read NON-CODE);
reverting the leaf to two-valued (below) turns **16** RED; unmutated, 581/581 pass. The `46/49 → 49/49`
figure above was re-measured on the final tree and controlled the same way: with the executable bit ignored
it reads **46/49**, so the number discriminates.

### The class-level rule: a predicate that feeds a safety decision must be TRI-VALUED

**Any predicate feeding a safety decision must be tri-valued — yes / no / could-not-measure — because a
boolean cannot express uncertainty and will therefore collapse it onto the permissive side.** That is the
durable rule out of this change, and it is a rule about the *shape* of the predicate, not about any one call
site: given only two values, "I could not tell" has nowhere to go but "nothing is wrong".

This was the **ninth** instance of the class on this PR alone — after `built-in-set: UNAVAILABLE`,
`corroboration: UNAVAILABLE`, the fail-open `${_census_end:-$_census_start}`, the permissive verdict scan,
and the measurement failures. What made the ninth instructive is the **level-shift**: the round-13 remedy
described immediately above made the **fold** order-independent *by construction*, and it worked — but it
left the **leaf** two-valued (`record=$(git ls-tree …) || return 1`), so a **failed lookup returned the same
value as a measured non-executable**. It proved the right property **one level too high**. An
order-independent fold over a predicate that has already discarded the distinction cannot recover it, which
is why a fourth point patch on the fold would not have ended the series. Reproduced with controls: a valid
repo → CODE; `REPO` not a git repository (every `ls-tree` fails) → **NON-CODE for a genuinely executable
file**; a bogus `BASE_SHA` with a valid HEAD → CODE, so the monotone OR did bound the blast radius to the
both-endpoints-unmeasurable case.

The leaf now returns three states, and the distinction that matters is *inside* the failure handling:

| lookup outcome | state | why |
|---|---|---|
| `ls-tree` succeeded, record is `100755` | **EXEC** | measured |
| `ls-tree` succeeded, record is another mode | **NOT-EXEC** | measured |
| `ls-tree` succeeded, **no record** | **NOT-EXEC** | measured — the path is genuinely absent at that ref (the added/deleted case) |
| `ls-tree` **failed** (not a repo, bad ref, corrupt object) | **UNMEASURABLE** | nothing was measured |

The join is the **maximum on the total order `NOT-EXEC < UNMEASURABLE < EXEC`**. Being a total order, the
join is associative, commutative and idempotent, so order-independence is now a property **of the lattice**
rather than of the loop — which is what keeps the fold's by-construction guarantee intact one level down.
EXEC dominates UNMEASURABLE *soundly*, not leniently: the rule is a disjunction, so positive evidence at one
endpoint already settles it and whatever the failed endpoint would have said could only be another "yes".
UNMEASURABLE dominates NOT-EXEC because "executable at neither endpoint" is a claim about *every* endpoint.
So **NOT-EXEC — the only state that reaches the permissive classification — is now reachable only from a
positive measurement at every endpoint**, and the accumulator starts at UNMEASURABLE so that an endpoint set
which yielded nothing cannot answer "prose".

An unmeasurable classification then **fails the run closed on `census-check:`** before anything is enqueued,
naming the path, the endpoint refs and git's own message, in the same "we cannot tell, never nothing to
review" wording the unresolvable base and the failed `git diff` already use. It is deliberately not spendable
as prose (`code-free:`) either — that would report an infra fault as a docs-only diff. Both functions were
**renamed** (`_roborev_mode_is_exec_at` → `_roborev_mode_exec_state_at`, `roborev_path_is_executable` →
`roborev_path_exec_state`) so that a surviving boolean call site, where `if` would silently re-collapse the
third state, breaks as a "command not found" instead of answering permissively.

Evidence: the leaf is probed directly through the real oracles file by a probe that itself prints **three**
outcome words (a boolean probe over a tri-valued function would print the defect as the expected answer), and
the consequence is driven end-to-end through the wrapper's summary block by fault-injecting a failing
`git ls-tree` via a PATH shim — legitimate because `ls-tree` has exactly one caller in the wrapper, so
nothing else in the run is perturbed. Both have controls in the opposite direction (the same fixture without
the shim reaches PASS and enqueues; the same repository read normally classifies correctly), and reverting
the leaf to `|| return 1` reds **16** assertions.

One piece of hygiene folded in: the single-path `ls-tree` lookup no longer passes `-z`. Captured through a
command substitution it made the shell warn `ignored null byte in input` on **every** call — harmless for a
single record, since only the terminating NUL is lost, but per-call stderr noise able to mask a real
warning. Only the leading MODE field is read, and it is first, space-terminated and always one of git's
literal mode constants, so the NUL delimiter buys nothing here; without it git C-quotes an odd name, which
keeps a newline-bearing path on one line. A `-z`-only mutant reds exactly one assertion — the
stderr-cleanliness one — which is the evidence that the removal is behaviour-neutral.

## Recommended design

### D1 — the config: artifact exclusions scoped to artifact-bearing DIRECTORIES (AC1)

`exclude_patterns` becomes: **`*.md` kept unchanged** (it already performs all prose exclusion,
repo-wide), and `docs/**` replaced by the **intersection of a non-code artifact extension and an
artifact-bearing directory** — `<artifact-dir-glob>/**/*.<ext>` over exactly four directory globs:

| directory glob | what it holds | artifact files (measured) |
|---|---|---|
| `docs/reports/*-artifacts` | per-issue measurement artifacts (the #3229 convention) | 577 |
| `docs/round-artifacts` | soak/round measurement output | 53 |
| `docs/**/jfr-reports` | JFR profiling output | 7 |
| `docs/sstables-definitive-guide/diagrams` | generated diagram renders | 30 |

crossed with the high-count raw-output and binary/image classes from the histogram: `txt json jsonl log
err csv png svg gz pdf jfr html mmd tex diff` — 4 × 15 + `*.md` = **61 patterns**.

(`<dir>/**/*.x` matches `<dir>/a.x` as well as `<dir>/a/b/c.x` — git's `**/` matches zero or more
components.) The consequence is that `.py`, `.sh`, `.bt`, `.c`, `.rs`, `.toml`, `.cql`, `.yml`, `.yaml`
under `docs/` are **reviewed**, which is AC1. Each of these patterns contains an interior `/`, so by R1 it
is **root-anchored** — nothing under `website/src/content/docs/` or any other nested `docs` directory is
affected. **None of them may be written with a trailing slash** (R3): a `docs/` form would invert to
recursive and re-widen the blast radius. That rule is now carried by the file's own comment block and by
doctrine, not by an automated check — the trailing-slash FAIL went with the oracle (D0) and belongs to
**#3283**.

#### D1a — why DIRECTORY-scoped, and why the intermediate `docs/**/*.<ext>` form was retired

The first revision of this change wrote the artifact patterns as `docs/**/*.<ext>` — an extension sweep
across **all** of `docs/`. That form does **not** satisfy this change's own stated asymmetry ("noise, never
blindness"), and round 6 retired it. **The asymmetry was originally written unqualified; this change
falsified it, and the corrected form is scoped:** it is sound for **inert dumps** — `.txt`/`.log`/`.err`
run output — where exclusion costs only **noise**. It is **false** for **code-bearing formats**
`.json`/`.html`/`.svg`, which can be *functional configuration under any path*, and for those exclusion is
**blindness**, not noise. Hence the rule the corrected form yields: exclusion of a code-bearing format must
be **scoped by directory, never by extension alone**. Two live cases in this repository prove it:

- **`docs/observability/grafana/dashboards/cqlite-overview.json`** — guarded by the **full agent gate's own
  `kit-dashboard-drift` component**, so the repository already treats it as correctness-bearing. Under
  `docs/**/*.json` a PR editing it was dropped from the reviewer's diff **and** classified code-free: the
  gate says "this is correctness-bearing" while the review path says "there is nothing here to review".
- **`docs/reports/delivery-telemetry.schema.json`** — the schema governing the delivery ledger, hidden the
  same way.

Measured on this branch: 672 tracked `docs/` files carry an artifact extension; **667** lie inside the four
directories above and remain excluded, and the **5** that do not are now delivered to the reviewer (the two
above, `docs/reports/delivery-telemetry.jsonl`, and two `sstables-definitive-guide` artifacts that sit
beside the prose rather than in `diagrams/`). With the scoping in place the asymmetry is **true as
written**: a *new artifact directory* is silently re-admitted to **review** (noise, a token cost), and
functional configuration under `docs/` can no longer be hidden (no blindness).

The doctrine surfaces state the corrected asymmetry **together with the counterexample that falsified it**
— naming `cqlite-overview.json` and the `kit-dashboard-drift` component that guards it — rather than as a
timeless claim, because a line that carries its own counterexample is much harder to re-break than one that
reads as always having been true. The generalisable part, worth more than this configuration: **an extension
describes a FORMAT; a directory records an INTENT** — someone decided that tree holds artifacts. That makes
a directory the better proxy for "generated", which is why the scoping is directory-first everywhere here.

**It stays extension-scoped WITHIN each directory — never a blanket `<dir>/**`.** That was the tempting
simplification and it would reintroduce this issue's original defect: these directories deliberately hold
executable code beside their output. Measured: **63** tracked `.sh`/`.py`/`.rs`/`.c`/`.bt`/`.cql`/`.yaml`/
`.toml` files under `docs/reports/*-artifacts/` alone, plus a `.py` under `docs/round-artifacts/`. Those
harnesses *are* the 136-path census `docs/**` swallowed on PR #3222.

The census-side mirror follows the same shape (`CODE_FREE_ARTIFACT_EXTENSIONS` ∩
`CODE_FREE_ARTIFACT_DIR_GLOBS`), matched **component-wise** to git's `:(glob)` semantics rather than with a
shell `case` — bash's `*` crosses `/`, so `docs/reports/*-artifacts/*` would also match
`docs/reports/a/b-artifacts/x`, which git's `*` does not. The globs are held in an **array**: as a
space-separated string, unquoted iteration pathname-expands them against `$PWD`, silently reducing the
classification to "the directories that exist in this checkout" (measured while writing it —
`docs/**/jfr-reports` collapsed to the single existing one and `docs/jfr-reports/a.html` stopped matching).

**Residual on the mirror, stated because it changed with the descope.** The census constants and the config
patterns are one fact written twice. The structural assert that derived the expected pattern set from the
constants and compared it for **set equality** against the committed `.roborev.toml` used the deleted TOML
parser, so it is **gone with the oracle**: a one-sided edit (a new artifact extension added to the config
but not to the census constants, or the reverse) is no longer caught mechanically. The failure direction is
bounded — a census/config disagreement now costs review **noise** (a file the config excludes that the
census calls code is simply absent from the prompt and surfaces under `prompt-content:`, fail-closed) — and
the remedy is the config comment's instruction to edit both in the same commit. Restoring a mechanical
mirror assert belongs to **#3283**, which will have a config reader again.

**A deny-list is forced, not preferred — and this is VERIFIED, not assumed.** `git.FormatExcludeArgs`
does only TrimSpace / TrimRight / TrimLeft / `Index`: there is no `!` handling and no re-include path at
the instruction level, and git pathspec supports none inside `:(exclude)` either. "Review these extensions
and nothing else" is therefore **not expressible**; the only lever is narrower excludes.

**The deny-list's known weakness, stated up front.** A *new* artifact extension appearing under `docs/`
is silently re-admitted to review prompts. That is a **token-cost** problem, never a correctness one:
the failure direction of a deny-list miss is "the reviewer sees noise". What guarantees it cannot invert
into "the reviewer silently sees nothing" is now the **directory scoping itself** (no pattern reaches
outside a tree whose whole purpose is committed run output) plus `prompt-content:`'s fail-closed check that
every code census path actually arrived — not a pre-enqueue prediction, which is deferred to #3283.

Three extensionless executables under `docs/` are not matched by an extension deny-list, so nothing removes
them from a review prompt — and that is now consistent with how the census classifies them: **CODE, because
git records them executable** (one of the three is a Python script; see the re-measurement above). git
renders the two ELF blobs as `Binary files … differ`, so the token cost stays bounded, and no by-name
exclusion is wanted: excluding them would restore exactly the silence this change closes.

**Operational risk to record:** `.roborev.toml` is a machine-managed file (`roborev config set` rewrites
it, comments and all). A rewrite that drops or reorders the list would silently restore the blind spot.
With the pre-enqueue detector deferred, **this is the residual risk this change knowingly carries**, and it
is why the hazard is named in doctrine (AC6) and in the file's own comment.

#### Alternatives rejected

| Alternative | Why it was rejected |
|---|---|
| **(a) Drop `docs/**` entirely, keep only `*.md`** | Correct on AC1 and the *simplest* change — but it admits ~570 raw run-output files (`txt`/`json`/`err`/`log`/`jsonl`) plus binary/image blobs into review prompts. A genuine review on a large diff already runs several hundred thousand input tokens; a report PR's artifact tree would blow past the prompt budget (roborev's own `max_prompt_size` fallback switches to *file paths only*, i.e. a degraded review) for zero review value. |
| **(b) Relocate the harnesses out of `docs/`** | **Explicitly ruled out by the owner in #3229.** Shipping a harness beside the report it produced is the convention and it stays. |
| **(c) Global (slash-less) exclusion of `*.txt`, `*.json`, …** | Per the recursive-normalisation finding, a slash-less pattern applies **repo-wide**. That would newly hide real config and data files elsewhere in the tree (`test-data/**/*.json`, workflow-adjacent JSON, fixtures) from review — a genuine regression traded for a shorter pattern list. |
| **(d) Ask roborev for an allow-list / negation** | Not expressible today (R5) and it is an upstream feature request on a binary we do not control. A worthwhile upstream ask; not this change. |

### D2 — REMOVED: the pre-enqueue reconciliation oracle (AC3, AC4 → deferred to #3283)

**This section is a record of a removal, not a description of a mechanism this change ships.** It is kept
in full because #3283 inherits the problem, and because a subsystem that four review rounds worked on
should not vanish without its history.

**What it did, in the past tense.** A pre-enqueue check read the effective `exclude_patterns` (worktree ∪
root ∪ global), constructed the git pathspecs as a port of `git.FormatExcludeArgs`, asked git which census
paths survived them, and FAILed — before anything was enqueued — when the census's CODE paths were not all
survivors, naming the swallowed paths, the pattern that ate each and the config file it came from. It
reported under its own greppable summary key, with a `PASS`/`FAIL` grammar and no `NOTICE` value. It also
FAILed on a trailing-slash pattern (R3) independently of whether anything was currently swallowed, and it
corroborated its TOML parse against `roborev config get exclude_patterns`.

**Why it is removed rather than fixed once more:** D0. Increasing false-PASS density (1, 1, 2, 3 across
rounds 8→11), two of round 11's three defects in code the immediately preceding fix rounds introduced, and
the class-level root cause that a port's correctness is only knowable by differential testing against the
original. **A guard with known documented false-PASSes is worse than no guard, because it invites reliance
it cannot support.**

**What the wrapper does instead.** Nothing predicts the exclusion set. `prompt-content:` expects **every**
code census path to appear in the prompt the reviewer actually received, with **no subtraction and no
excusal** (D5), so a swallow of any origin — a configured pattern, a compiled-in built-in, an upgrade that
changes `FormatExcludeArgs` — surfaces as a **FAIL after the review round**. The cost is a diagnostic that
names the symptom ("the reviewer never received their diffs" — which is TRUE) rather than the mechanism.
The direction is fail-closed: never a vacuous green, never a merge on unreviewed code.

**AC3 and AC4 are therefore DEFERRED to #3283** — not satisfied (neither by mechanism nor, for AC4, through
its second "document the residual" branch), not waived, not unmet. The delta spec removes the two ADDED
requirements that carried them, so nothing lands in `openspec/specs/` half-satisfied.

**A NOTE ON AC4's SECOND BRANCH, because an earlier revision argued it differently.** While the oracle
existed, this design argued that AC4 was met through its second branch (*"…or the residual disagreement is
documented with the exact cases where it persists"*) by documenting the compiled-in built-in deny-list as a
declared, pinned, fail-closed residual — and it argued that taking the disjunction's second branch was not
a coverage loss. **That reasoning is VOID.** It applied to a disjunction whose *first* branch was in place:
a live reconciliation of the census against the configured set, with the built-in as the only declared gap.
With the reconciliation itself deleted there is no branch left to satisfy, and AC4's status is `deferred`
outright.

#### D2a — roborev's compiled-in BUILT-INS are separate, unmodelled, and tracked as #3278

**The fact.** `exclude_patterns` is not the whole exclusion set. The binary ALWAYS appends a hard-coded
lockfile/cache deny-list — `**/Cargo.lock`, `**/cargo.lock`, `**/go.sum`, `**/pnpm-lock.yaml`,
`**/package-lock.json`, `**/packages.lock.json`, `**/yarn.lock`, `**/bun.lock`, `**/bun.lockb`,
`**/pdm.lock`, `**/uv.lock`, `**/poetry.lock`, `**/Pipfile.lock`, `**/Gemfile.lock`, `**/composer.lock`,
`**/flake.lock`, `**/mix.lock`, `**/pubspec.lock`, `**/Podfile.lock`, `**/Package.resolved`,
`**/.beads/**`, `**/.cache/**`, `**/.gocache/**`, `**/.kata.local.toml` — to the pathspecs it hands git,
with no configuration switch, no opt-out and no negation form. A census path one of those eats is exactly
as invisible to the reviewer as a configured swallow.

**The decision (owner-ruled, #3229): not modelled here; deferred in full to #3278.** Modelling it was
built and then removed one round before the wider descope, for the same reason and with the same evidence:
four consecutive review rounds each found a false-PASS **inside that subsystem alone** — a phantom `/**`
sibling on a pre-formatted pathspec constant; an unbounded substring presence test (`**/Cargo.lock` matched
inside `**/Cargo.lock.bak`); a three-state `OK`/`DIVERGED`/`UNAVAILABLE` signal tested as two, so
`UNAVAILABLE` took the permissive branch; and the excusal machinery itself, which let coverage be excused on
a model that could not be verified.

**The residual this leaves, stated:** a diff whose code-census paths include a path the compiled-in
deny-list excludes (`Cargo.lock`, `go.sum`, `pnpm-lock.yaml`, `Gemfile.lock`, `package-lock.json`, or
anything under `.cache/`, `.beads/`, `.gocache/`) has that path **silently dropped from the reviewer's diff**,
and it surfaces as a `prompt-content:` **FAIL** — the symptom, not the mechanism. It **fails CLOSED**. The
degenerate sub-case (a dependency bump whose only code-census path is a lockfile) also FAILs. **#3278**
owns closing the diagnostic gap.

**Measured consequence for #3096.** #3096 is held `HOLD: merge after #3229` partly on **F1**, the built-in
mis-modelling behind its `Cargo.lock` residual, and its worker was told to expect a numeric
`prompt-content: PASS (n/n)`. F1 lived inside the removed subsystem, so that expectation no longer holds:
measured on a hermetic #3096-shaped fixture while the machinery still existed, a #3096-shaped diff yields
**`prompt-content: FAIL`** (option **(iii)**), not a numeric PASS and not a PASS-with-residual. Re-anchoring
#3096 is the owner's call, not this change's.

**Findings deleted with their subject, not waived.** The High finding about the excusal mechanism (job 33,
H1) has **no subject remaining**: the code it described is gone. That distinction matters because a High
finding that vanishes because its subject was removed reads *identically* to a waived one unless it is
written down — nothing was excused, judged acceptable, or deferred-with-the-defect-still-present.

#### D2b — the lessons the removed subsystem taught, kept because they generalise

These are the transferable findings. They describe code that no longer exists; they are here because each
is a repo-level blind spot reproduced in shell, and because #3283 will meet all of them again.

**1. The multi-state-signal shape: a signal where only the BAD states are tested, so every unknown or
unmeasured state inherits the PERMISSIVE branch.** Four sightings, indistinguishable when written together:

| # | signal | states | which were tested | what the UNMEASURED state did |
|---|---|---|---|---|
| 1 | the built-in-set state | OK / DIVERGED / UNAVAILABLE | `= DIVERGED`, `!= DIVERGED` | `UNAVAILABLE` took the permissive **excusal** path — coverage excused on an unverified model |
| 2 | the corroboration state | OK / DRIFT / NOTICE / **UNAVAILABLE (initial)** | `= DRIFT`, `= NOTICE` | `UNAVAILABLE` reached a `PASS (no exclusion patterns configured)` and **enqueued** a review |
| 3 | a suite-internal scan end-line | a line number / empty | a `${:-$start}` default | a failed `awk` degraded to a **1-line scan**, in which the absence-assert reads `ok` |
| 4 | a details helper switching on the built-in state | OK / UNAVAILABLE / … | two arms, **no `*)`** | an unhandled state produced no diagnostic at all |

Instance 2 is the sharpest, because the code's own comment stated the correct principle three lines above
the defect — *"our parser recognised no key" is NOT "nothing is configured"* — and then never required the
oracle that could tell them apart to have **answered**. **The rule extracted from it, which is retained and
now enforced structurally in the wrapper (D4):** *a positive verdict requires an affirmative measurement*;
an oracle that is the SOLE evidence for a claim and could not be consulted yields a NON-PASSING verdict
whose text distinguishes "we could not check" from "nothing was wrong"; a permissive branch is keyed on the
AFFIRMATIVE value, never on the absence of a bad one; and where a signal genuinely SHOULD be permissive, the
reason is recorded IN CODE at the branch.

**2. A hermetic suite that MIRRORS a production constant is a SYMMETRIC oracle, so it cannot catch an error
both sides make.** The deleted built-in mirror was first written as a space-separated STRING iterated
unquoted, so bash **pathname-expanded** it (`**/package-lock.json` became the repo-relative
`website/package-lock.json`) and the check reported a false FAIL on every run. The regression suite's mirror
was *also* a space-separated string iterated unquoted — **both sides made the identical mistake**, so the
planted literals and the presence check agreed with each other and the key passed. That is #3042's rule (a
symmetric producer/consumer test is invariant to a uniform error; two defects that cancel are undetectable
*by construction*) reproduced in shell rather than in SSTable framing. What exposed it was the only
asymmetric oracle available: the **real roborev binary**. The generalisable rule: a mirror needs an
ASYMMETRIC check or a structural assert against the production file itself. (See D1a's residual: the mirror
assert that applied this rule to the census/config pair used the deleted TOML parser and is gone with it.)

**3. A test that blesses a vacuous verdict is WORSE than an unguarded path.** Two cases in the suite once
**locked in** an un-corroborated `PASS (no exclusion patterns configured)` — the exact state a silently
self-disabled guard produces — so a green suite blessed a self-disabled guard. An unguarded path is merely
unprotected; everyone can see there is no check. A test asserting the passing value for that state is
harmful twice over: it **consumes the review budget** that would otherwise have looked at the path, and it
converts "nobody checked" into "we checked and it was fine" — the one statement that stops anyone looking
again. When adding a case whose expected value is a PASS, ask *what state the system is in when that PASS is
wrong*, and make the fixture distinguish the two. **This rule is why the descope also deleted the fixtures'
ability to write a `.roborev.toml`**: nothing reads one now, and an inert input that reads as load-bearing is
the same class of misleading test.

**4. The PRE-EXISTING guard caught the NEW guard. Keep uncorrelated layers.** The removed oracle once
reported `PASS (7/7 code census paths survive)` about a config roborev never read. What caught it was
`prompt-content: FAIL (1/7 code census paths absent)` — the **older**, cruder guard, whose whole cost is
that it only fires *after* a review round is paid for. Defence in depth is usually justified as "the new,
sharper check will catch what the old one misses"; here the new check was the wrong one. **A layer is worth
keeping not because it is better than the other, but because its failure modes are UNCORRELATED with the
other's.** The corollary is why `prompt-content:` is the layer that survived the descope: the cheap early
check can be wrong about its INPUT; the expensive late check reads what actually happened.

**5. An empty parse must be corroborated, not trusted — and a genuinely quoted key spelling is reachable.**
Measured against roborev v0.61.2: a config containing the **quoted key** `"exclude_patterns" = [...]` —
valid TOML, the same key — is honoured (`roborev config get` answers it), while a bare-key pattern match
skips the line entirely. A guard that then reports "nothing configured" enqueues a review from which every
`docs/reports/*-artifacts/**` executable is silently dropped: **#3229 reintroduced under the key whose whole
job is preventing it.** #3283 must treat "our parser recognised no key" and "nothing is configured" as
different states, and must not enumerate spellings as its primary defence — enumeration is a list to keep
complete, and the cross-check against the binary is the load-bearing half.

**6. A probe is only evidence while it DISCRIMINATES.** The root-anchoring probe's evidence is its
*survival*, and survival means something only if the two candidate readings of a configured pattern
**disagree** about the path. At its original location the discriminating pattern was the pre-round-6
`docs/**/*.json`; round 6's directory-scoping deleted that pattern, after which the old path survived under
*both* readings — **vacuous evidence**, which is worse than absent evidence because it reads exactly like
the real thing. The probe was relocated to a path a *currently configured* pattern discriminates (D6). **Any
future change to `exclude_patterns` must re-check that discrimination**, because deleting a pattern can
silently retire the probe. And do **not** substitute hand-rolled `git ls-files` + `:(exclude,glob)`
pathspecs for the check: measured on this issue, `git ls-files -- 'website/' ':(exclude,glob)*.md'` returned
**0 of 95** files, because an exclude pathspec combined with a *literal file* pathspec returns 0
unconditionally — either answer would have manufactured a configuration defect that does not exist.

### D3 — RETAINED: ONE canonical path-normalisation boundary

**The pattern is the finding.** Rounds 2, 3 and 4 of review produced **six blockers and every one was a
path-normalisation defect** — in a different consumer each time: the oracle compared paths from the wrong
config source; a total exclusion swallow certified an empty prompt; `prompt-content:` could not parse
space-bearing or C-quoted headers; the **census classified a C-quoted path by its quoted spelling**; rename
and mixed-quoted headers were unreachable; a newline-delimited path set turned one path into two grep
alternatives. The root cause was structural: **normalisation was scattered.** Patch the reported consumer
and the next round finds the next consumer. So the design changed the shape of the problem instead of the
symptom, and **that boundary is retained in full** — it is what `prompt-content:`, the surviving
anti-vacuity key, reads.

**THE BOUNDARY.** Paths are normalised **once, at the census**, by asking git for them **NUL-delimited**:

| Source | Rendering of `docs/é.sh` | Rendering of `docs/a b.sh` |
|---|---|---|
| the census (`git diff --numstat -z`) | `docs/é.sh` (RAW) | `docs/a b.sh` (RAW) |
| the prompt's diff header (produced by roborev, not by us) | `diff --git "a/docs/\303\251.sh" "b/…"` | `diff --git a/docs/a b.sh b/docs/a b.sh` |

With `-z` there is **no quoted spelling to reconcile** on any git-sourced path: `census_paths` /
`census_code_paths` hold the same RAW bytes, and RAW is the single representation used for classification,
comparison **and** display. The census's extension/prefix tests see `md`, not `md"`. Records are read with
`read -r -d ''`, so a path containing a NEWLINE survives — something a line-oriented read cannot do at all.

The only text that still arrives quoted is text **we did not get from git plumbing**: the reviewer's prompt.
So `roborev_unquote_path` exists with exactly **one caller**, `roborev_diff_header_has_path`, and that
matcher is the **only** way any consumer may ask "is this census path in the prompt?".

**THE MATCHER, and why each shape is decidable.** A quoted side is unambiguous (a C-quoted body holds no
unescaped `"`, so the first unescaped one ends the token, spaces and all); an unquoted side holds no `"` at
all when git wrote it. That yields four shapes, three of them exactly parseable:

1. `diff --git "a/<q>" "b/<q>"` — both quoted: both sides decoded exactly.
2. `diff --git "a/<q>" b/<raw>` and `diff --git a/<raw> "b/<q>"` — **MIXED**, emitted when only one side
   needs quoting. This occurs **only on renames**, which is why a both-sides-quoted parse never reached it
   and both census sides were reported absent. (Confirmed: `--no-renames` is absent from the roborev
   binary's strings, so the reviewer's diff has rename detection ON while our census splits renames.)
3. `diff --git a/<raw> b/<raw>` — genuinely ambiguous when a name carries a space (`a/x y b/z w` has several
   readings), and **no reading of the header LINE can settle it**.

**AMBIGUITY IS RESOLVED FROM EVIDENCE, NOT FROM POSITION.** The first attempt at shape 3 tested each
**position** the wanted path could occupy — `case $rest in "a/$want b/"*)` and friends. That is a **PREFIX
test**, and it produced a genuine **FALSE PASS in the merge gate**: with a tracked file named `foo b/x`, the
header `diff --git a/foo b/x b/foo b/x` has `a/foo b/` as a prefix, so the *unrelated* census path `foo`
read as PRESENT. Reproduced: `roborev_diff_header_has_path 'diff --git a/foo b/x b/foo b/x' foo` → present.
The code carried a comment asserting this was impossible; the claim was false, and the comment is corrected
in place rather than deleted, because the next reader relies on it.

**Failing closed was NOT an option.** With renames ON the header ambiguity is irreducible
(`a/foo b/(bar b/foo b/bar)` is a legal reading), so refusing to decide would red **every space-bearing
header** and reintroduce the round-3/4 false FAILs pinned by `cx6c`, `cx6g`, `cx6h`. Instead: **git does not
leave renames ambiguous elsewhere in the diff.** For a rename or copy it always emits
`rename from <path>` / `rename to <path>` (with `similarity index`) immediately after the header — one path
per line, C-quoted when needed, so each is exactly decidable. And for a non-rename the two header paths are
**identical**, which disambiguates by itself. Hence the resolution order:

| # | evidence | ruling |
|---|----------|--------|
| 0 | the header's own `rename from`/`rename to` (or `copy from`/`copy to`) lines | AUTHORITATIVE — the header line is not consulted at all |
| 4a | otherwise, some valid `a/<A> b/<B>` split has **A == B** | the header is a NON-rename (git always writes rename lines for a rename), so ONLY the equal reading counts |
| 4b | otherwise (no equal split, no rename lines) | it can only be a rename whose rename lines did not reach us: any valid split counts — the **declared residual** |

Every candidate split is *enumerated*, and `$want` is **byte-compared**, never used as a pattern, so a path
containing `*`, `?` or `[` matches literally. On the reproduction, 4a fires (`foo b/x` == `foo b/x`) and `foo`
is correctly ABSENT; on `cx6g`'s space-bearing rename no split is equal, so 4b keeps it PRESENT.

**The residual is declared, not implied.** 4b is permissive: a `$want` that is one side of *some* valid split
reads PRESENT even if the producer meant another split. It is reachable only for a header that (i) carries a
space, (ii) names two DIFFERENT paths — i.e. is a rename/copy — and (iii) arrived **without** the rename lines
git always writes. For git's own output it is unreachable, and it is the price of not re-breaking `cx6g`/`cx6h`.

**The matcher's INPUT widened; the boundary did not move.** Resolving from the rename lines means considering
the lines *following* a header, so header collection lives in the oracles file beside the matcher
(`roborev_collect_prompt_headers`, awk-based so a multi-megabyte prompt is not read in a bash loop, with the
extended-header run BOUNDED so a `rename from` in the reviewer's prose or a diff body line is never
attributed to a header). The invariant is preserved exactly: `roborev_unquote_path` still has one caller, and
`roborev-review-checks.sh` still performs no unquoting and holds no header-shape knowledge.

Membership is decided **per header, in bash** — no regex, no path-set file, no `grep -Fxq` over
newline-delimited paths. That is what closes the newline false PASS: with census `{a, a<LF>b.rs}` and a
prompt naming only `a`, the old set-and-`grep` mechanism reported `PASS (2/2 present)` because a multi-line
pattern is a list of alternatives. The path is now either named by a header or reported ABSENT.

**The false-FAIL direction is the dangerous one.** `prompt-content:` is the wrapper's strongest deterministic
anti-vacuity key; a key that reds on correct input is the key agents learn to waive, and a waived
`prompt-content:` defeats the entire purpose of this change. Reachability is not theoretical: the repository
tracks `docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md`, **40 space-bearing paths under
`docs/`** including the directory `docs/storage engine/`, and this change *promotes*
`docs/reports/*-artifacts/**` executables to CODE census paths.

**THE INVARIANT IS PINNED STRUCTURALLY, because that is what stops the next round.** Behavioural cases can
only cover the shapes someone thought of — and each round proved someone had not. The guard suite therefore
asserts the boundary itself: every path-reading `git diff` carries `-z`; the census does not normalise inside
its classification loop and reads NUL-terminated records; the quoted-path decoder is defined once and called
**only** from inside `roborev_diff_header_has_path`; and the three retired mechanisms (a `[^ ]+` header
regex, the `.promptpaths` set file, `grep -Fxq` membership) are absent from the executable lines of the
consumer. Each assert was verified to FAIL under a deliberate mutation, so it is a live check rather than
decoration.

**Test-quality consequence.** One case that named this behaviour once asserted only the (now removed)
exclusion key and so reported two `ok`s while `prompt-content:` false-FAILed and the run terminated
`RESULT: FAIL` — hundreds of asserts green over a broken key. *A case that passes while the behaviour it
names is broken is worse than no case, because it is read as coverage.* Every hostile-path case now asserts
`RESULT:` **and** `prompt-content:`, and the suite's stub JSON-escapes the prompt so a quote-bearing prompt
cannot degrade the job record and mask the comparison.

#### D3a — no path may reach a summary value un-neutralised

The block is **line-oriented** and it is what every reader keeps: `flow-closer`, the flow-* skills and this
repo's own guard suite retain only the block and grep it by `^<key>: ` / `^RESULT: ` to decide whether a
merge proceeds. Diff-derived text reaches those values — details name paths — and **a census path is
attacker-controlled**: it is whatever a PR chose to track. A filename carrying a NEWLINE therefore let a
value **span lines** and introduce arbitrary `key:` lines, up to a forged `RESULT: PASS`, into the block
whose entire purpose is to be trusted. Measured on the mutant: **3 `RESULT:` lines, one of them
`RESULT: PASS`**, plus a forged `prompt-content: PASS`.

**Fixed at the single emit boundary, not per interpolation site.** `emit_kv` (every block value) and
`finish` (every detail line) run their text through `roborev_safe_line`, which renders control characters as
visible escapes (`\n`, `\r`, `\t`, else `\ooo`). A per-site escape is a list to keep complete — the next
value to grow a path interpolation reopens the hole silently — so the property is **total** and holds for
keys that do not exist yet. Structural asserts pin the boundary itself (every `emit_summary` value goes
through `emit_kv`; all **22** keys do (21 keys plus the terminal `RESULT:`, which goes through the same
boundary); `emit_kv` neutralises; `finish` neutralises and no longer bulk-prints
`"${DETAILS[@]}"`), each verified to FAIL under mutation.

Two deliberate choices:

- **Quotes, backslashes and spaces are left intact.** The block names paths **by their real bytes** —
  `docs/…/odd "q" name.sh` must still read as itself, pinned by `cx6b` — and no non-control byte can begin a
  line or a `key:`. Escaping more would trade a real diagnostic for no extra safety.
- **The rendering is not reversible, and that residual is declared.** A path holding the two literal bytes
  `\` `n` renders the same as one holding a newline. The guarantee is exactly *no value spans a line and no
  `key:` can be introduced*; a caller wanting exact bytes reads them from git, not from a summary block.

### D4 — RETAINED AND STRENGTHENED: the closed verdict grammar and the affirmation backstop

The wrapper's terminal verdict scan was **the same permissive shape** as D2b's lesson 1, at its most
consequential point: four failing prefixes tested, everything else falling through to a PASS. Two closures
answer it, and **both are retained through the descope and strengthened**:

1. **The non-failing set is an ALLOW-LIST with a failing fallback** (`cx28`). A per-check key holding a
   value outside the documented grammar FAILs, naming itself, so an unplanned value cannot inherit the
   non-failing branch.
2. **A PASS additionally requires every deterministic key to be affirmatively `PASS`** (`cx29`) — closing
   the neighbouring case where a value is IN the grammar and non-failing but is not a MEASUREMENT, namely
   the initial `SKIP` of a check that never ran. Un-backstopped, an early-returning `prompt-content:`
   PASSED the run with the strongest anti-vacuity key having measured nothing.

**The strengthening: EXACT TOKEN MATCHING, never a prefix glob.** Both closures now compare the **verdict
token — the value up to its first space — EXACTLY**. A `PASS*` glob accepted `PASSthisNeverRan` and
`PASS-MEASUREMENT-DID-NOT-HAPPEN`, i.e. the backstop against unmeasured keys was itself satisfiable by a
value that measured nothing: the closure was checking a **spelling** rather than a **state**. Two guard
cases pin the near-prefix mutants — `cx28b` (`PASSthisNeverRan`, a token glued to more characters with no
separator) and `cx28c` (`PASS-MEASUREMENT-DID-NOT-HAPPEN`, a token followed by a hyphenated state name).
A mutation reverting to prefix globs makes **both** mutants reach `RESULT: PASS`, which is what proves the
asserts bite rather than decorate.

**The backstop names SIX deterministic keys with NO per-key exemption**: `push-assert`, `census-check`,
`code-free`, `sha-assert`, `review-completed`, `prompt-content`. The one exemption that ever existed
belonged to the removed exclusion key (a `NOTICE` was allowed there while a remedy-less built-in swallow was
a measurement with a stated residual); both the key and its exemption are gone, so the backstop is now
**uniform, which is STRICTER, never weaker**. A structural assert reads the backstop's own `case` body and
requires **exactly one** exempting arm — the affirmative `PASS)` one — so no per-key hatch can be
reintroduced. `vacuity-tier1/2` and `findings:` are deliberately excluded from the backstop: they
CORROBORATE, and `UNAVAILABLE` / `NONE` are documented legitimate values for them on a clean run.

#### Decision record: why RETAIN this, when the oracle that surfaced it was deleted

| Option | Verdict |
|---|---|
| **(i) Remove the closures with the oracle** | **Rejected.** The permissive verdict scan is a **PRE-EXISTING** defect of the #2964 wrapper that this change's sweep *found*; it is not something the oracle introduced, and it does not depend on the oracle in any way (its subject is the six keys that remain). Removing it would leave the wrapper **worse than we found it** — a bad trade, and one that would have to be re-done from scratch by the next issue that trips over a `SKIP` riding to PASS. The descope's principle is *subtraction cannot add a false-PASS*; deleting these closures would do exactly that, since a non-measurement value would again reach `RESULT: PASS`. |
| **(ii) Keep the closures but keep the `PASS*` prefix globs** | **Rejected.** A prefix glob makes the closure a spelling test: `PASSthisNeverRan` satisfies it. That is a **known documented false-PASS inside the guard**, and this change's own governing sentence applies to it — a guard with known documented false-PASSes is worse than no guard, because it invites reliance it cannot support. Fixing the match is a two-line change with two pinning cases; keeping a hole we had measured would be indefensible while deleting an entire subsystem for having holes. |
| **(iii) Exact-token match, six keys, no exemption (chosen)** | The closure now tests a **state**, not a spelling; the exemption mechanism is gone rather than merely unused; and both properties are pinned behaviourally (`cx28b`/`cx28c`) and structurally (the one-exempting-arm assert). |

### D5 — `prompt-content:` expects EVERY code census path, and subtracts nothing

There is **no subtraction and no excusal**. No key is licensed to tell `prompt-content:` which census code
paths to skip: nothing computes a "known absence" set at all, because nothing models roborev's exclusions.
A path the reviewer really did not receive therefore FAILs — the fail-closed direction, and the residuals
documented in D2/D2a are exactly what lands here.

That is a deliberate reversal of an earlier design in which the built-in-excluded set was handed down and
`prompt-content:` reported `(+<n> not expected: …)`. The excusal was the mechanism behind two of the four
false-PASSes in D2a's history (it excused coverage on a model that could not be verified), and it was the
ONE place in the wrapper where one key could weaken another. Removing it means the guard may now report a
FAIL whose stated cause is imprecise; it can no longer report a PASS that covered less than it claimed.

**And the floor stays: a `0/0` is never a pass.** With no subtraction, `census_total == 0` means the census
had no CODE path at all — which `code-free:` already FAILs pre-enqueue — so the branch is unreachable
through the normal ordering. It is kept anyway, as a **structural** backstop that does not depend on an
upstream check still being there, because `PASS (0/0 code census paths present)` is textually
indistinguishable from a genuine pass. Its regression case (`cx21`) therefore drives the function
**directly** — a test that could only be written against the current control flow would evaporate with the
next refactor.

### D6 — the demonstration is a recorded POST-MERGE run, and the primary evidence is a real PR (AC2)

AC2 is satisfied by **running** the sanctioned wrapper, not by reasoning about it. But it cannot be run
against *this* change's own review.

#### Why it cannot be pre-merge

roborev resolves `exclude_patterns` from the **ROOT checkout** and **snapshots it at daemon start**. So
while this change is unmerged, the set applied to its own review is the root checkout's pre-change blanket
`['docs/**', '*.md']`. The original plan committed an **executable under root `docs/`** precisely so the PR
would be a #3222-shaped demonstration of its own fix — but under the old set that executable is swallowed,
so the demonstration was a **deadlock, not a test**: *the specimen that proves the fix is the specimen the
unfixed configuration eats.* (While the removed oracle existed it FAILed correctly on exactly that
specimen, which is what identified the deadlock.) So the executable is removed from the branch and the
procedure kept as committed prose (`docs/reports/3229-artifacts/live-probe-procedure.md`). The requirement
is **rescheduled, not dropped** — and the reason is recorded, because a quietly weakened acceptance
criterion is indistinguishable from one that was never met.

`website/src/content/docs/reports/_3229-artifacts/_3229-root-anchoring-probe.json` **stays** on the branch:
a `.json` under a *nested* `docs/reports/*-artifacts/` directory is not swallowed by the configured set
(root anchoring, R1), so it does not deadlock and is live evidence either way. Its path is chosen to
**discriminate**: the configured `docs/reports/*-artifacts/**/*.json` matches it under the incorrect
`**/`-prefixed reading and NOT under the correct root-anchored one, so its survival is evidence rather than
decoration. Both path segments Astro would otherwise pick up are underscore-prefixed, and `_3229-artifacts`
still matches `*-artifacts`, so the file is inert to the website build AND discriminating. See D2b's lesson
6 for how that discrimination was established and why it must be re-checked whenever a pattern is deleted.

#### The primary evidence is a real PR, not the probe

A probe is written to pass. The first post-merge PR that *happens* to carry an executable under `docs/`
proves the fix on a diff **nobody shaped for it**, which is strictly better evidence and costs nothing
extra — #3234 ships harnesses now, #3096's successor will, #3249's artifacts may.

- **AC2's record** = that PR's `census:` + `prompt-content: PASS (<n>/<n> code census paths present)` lines,
  posted to #3229. (The `census-exclusion:` line an earlier revision expected here no longer exists: it was
  the removed key. `prompt-content:` is the one that says the reviewer actually received the executables,
  which is the property AC2 is about.)
- **The committed procedure** = the documented **fallback**, if no such PR arrives promptly or its evidence
  is ambiguous.

#### The named trigger — an unowned post-merge obligation is not an obligation

Post-merge intentions decay, and this project has the receipts: **#3232** existed only as prose in #3100's
close; **#3103** shipped while its producer stayed uncommitted, after which three separate issues rebuilt a
corpus. So the obligation carries mechanism:

1. On merge, **#3229 goes to `In Review`, NOT `Done`** — `Done` auto-closes the issue and the obligation
   would vanish with it.
2. The PR is finalized and delivery telemetry stamped as usual; neither waits on the demonstration.
3. #3229 flips to **`Done` only once the AC2 evidence is posted** on the issue.
4. If the demonstration has not happened **within a few days**, it is **filed as a tracked issue** — never
   left to live in a comment thread.

Before running it post-merge: update the ROOT checkout **and restart the roborev daemon** (it snapshots
config at start; the one observed had 4d15h uptime).

#### Reading the tokens: the mechanism's thresholds, not a memorised band

Earlier drafts of this design enshrined a **398k–649k input** "genuine-review band". That is wrong as a
threshold and has been corrected. Judge against the wrapper's own thresholds, which are what the verdict is
actually computed from:

- **`input` ≥ `ROBOREV_VACUITY_MIN_INPUT_TOKENS` (25,000)** — anchored on the *highest observed vacuous
  run* (18,801), with headroom. Below it, tier 2 FAILs.
- **`cached` > 0** — a vacuous run measured exactly 0 cached.
- **`output` is ADVISORY ONLY, never a failure condition** — and the reason is decisive: a genuine **clean**
  review emits roughly **20–60** output tokens, *indistinguishable* from the vacuous baseline's 53–56.
  Output therefore cannot be a realness test on its own, in either direction. Already documented at
  `scripts/flow/roborev-review-checks.sh:328`.

398k–649k is cited **only as observed on large diffs**. It is diff-size dependent, and an absolute floor
drawn from large-diff observations would **falsely flag legitimate small diffs**: a real, substantive round
measured during this change was `input=118514 cached=88320 output=5954` on a ~90k-character prompt, with two
substantive findings citing real code — unambiguously genuine, and far below that band.

The vacuous **signature** to recognise is a shape, not a magnitude: input below the 25k floor, `cached == 0`,
a few dozen output tokens in seconds. PR #3222 measured 15,443 in / 89 out beside
`prompt-content: FAIL (136/136 code census paths absent)`.

The demonstration needs the network and a live reviewer, so like the #2964 worktree probe it is
**documented and recorded, never gate-run**.

### D7 — the wrapper's surfaces after the descope

`scripts/flow/roborev-review-oracles.sh` keeps `roborev_census`, the `code-free:` classification, the
docs-scoped `CODE_FREE_ARTIFACT_EXTENSIONS` / `CODE_FREE_ARTIFACT_DIR_GLOBS` constants and the
path-normalisation boundary (`roborev_unquote_path`, `roborev_collect_prompt_headers`,
`roborev_diff_header_has_path`). It no longer contains any config reader, TOML parser, pathspec port,
corroboration oracle or survivor query.

`scripts/flow/roborev-review.sh` loses the removed key's state variable, its `emit_summary()` line, its call
site and its verdict-scan registration, and carries the exact-token grammar + affirmation backstop of D4.
**The summary block is back to 22 keys** (from 23) — 21 keys plus the terminal `RESULT:` — in the same
fixed order otherwise. The removal is
asserted **structurally in both directions** — the key is absent from the verdict-scan key list *and* from
the emit line — so it is visible in the **OUTPUT contract**, not only in the source; and the deleted
functions are asserted absent by name.

`scripts/flow/roborev-review-checks.sh` keeps `prompt-content:` with no subtraction (D5).

**Campsite note:** the wrapper stands at 1047 lines and the oracles file at 721 — the wrapper is over the
~800 target, so any further growth there must be a split by responsibility (#1116) rather than more inline
logic. A fourth sourced file was considered and rejected while the oracle existed (each additional sourced
file adds another missing-or-truncated fail-closed validation surface); with the oracle gone the question is
moot until #3283 reopens it.

### D8 — hermetic regression tests (AC5)

`scripts/tests/test_roborev_review_guard.sh` is fully hermetic: a stub `roborev` written first on `PATH`,
`STUB_*`-driven, with a hermeticity meta-assert and no network / cargo / real reviewer. Tally after the
descope: **477 passing assertions, 0 failed** (from 644 — the removed oracle's case families went with it,
and `cx28b`/`cx28c` were added). The `(cx*)` family covers:

1. **executables under `docs/` are CODE, and REACH the reviewer** — `.py`/`.sh`/`.bt` under
   `docs/reports/x-artifacts/` ⇒ `code-free: PASS`, `prompt-content: PASS`, review IS enqueued (`cx1`);
2. **prose-only under `docs/`** ⇒ `code-free: FAIL` and `assert_never_enqueued` — the #2964 behaviour is
   preserved, not loosened, and this is the case that proves the change did not trade one blind spot for
   the opposite one (`cx2`);
3. **docs artifacts only** (`.txt`/`.json`/`.log`/`.err`) ⇒ still `code-free: FAIL`, never enqueued (`cx3`);
4. **every diff-header shape git emits** (D3) — a space-bearing directory (`docs/storage engine/`), a
   non-ASCII octal-escaped name (`é.sh`), the escaped-quote shape, MIXED-quoted renames, a newline-bearing
   path reported ABSENT and the same path PRESENT when its header is there (`cx6`, `cx6c`–`cx6k`). Plus the
   test-quality rule these cases exist to enforce: **a hostile-path case asserts the terminal `RESULT:` and
   `prompt-content:`, never one intermediate key alone**, and the stub emits VALID JSON for a quote-bearing
   prompt so the record cannot degrade and mask the comparison;
5. **the header-ambiguity resolution, both directions** — `cx6l` (a tracked `foo b/x` beside a `foo`, prompt
   naming only the former ⇒ `prompt-content: FAIL (1/2 … absent)`, so a PREFIX reading can never stand in
   for a delivery); `cx6m` (an ambiguous rename header **plus** its `rename from`/`rename to` lines ⇒
   `PASS (2/2 present)`); `cx6n` (**the same header with those lines removed** ⇒ `FAIL`, which is what
   proves `cx6m` is carried by the rename lines and not by a permissive positional fallback);
6. **verdict forgery by filename** (D3a) — `cx6p`: a path whose NAME carries newlines plus `RESULT: PASS`
   and `prompt-content: PASS` lines ⇒ **exactly one** `RESULT:` line, no forged key anywhere, and the path
   still NAMED with its newlines escaped on one line. A dedicated `assert_one_result_line` helper is
   required: `assert_verdict` reads `^RESULT: ` | `tail -1` and is therefore BLIND to an injected verdict
   line above the real one — only a count can see it;
7. **the `0/0` floor** (`cx21`), driven **directly** against `roborev_check_prompt_content` in the real
   files (the state is unreachable through the wrapper because `code-free:` FAILs first) — asserts the
   refusal value AND the *absence* of any `PASS (0/0` form;
8. **the CLOSED verdict grammar and the affirmation backstop** (D4) — `cx28` (a per-check key holding a
   value outside the documented grammar ⇒ FAIL naming itself), `cx28b`/`cx28c` (the near-prefix mutants
   `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN` ⇒ FAIL, so the closure tests a state and not a
   spelling), `cx29` (a check that returns before assigning its key ⇒ FAIL, so an un-run check cannot ride
   to PASS on its initial `SKIP`). These run patched COPIES of the three flow scripts, each with the
   UNPATCHED copy shown to PASS first — an assert that a copy FAILs is otherwise satisfied by a copy that
   failed because it was copied wrong, which is a probe failing in the direction that looks like success.
   Plus structural asserts read from the scan and backstop STATEMENTS (never a file-wide grep, which the
   summary block's own emit line would satisfy): the failing-capable set is exactly
   `FAIL|FINDINGS|ERROR|INCONSISTENT` (exact tokens; `NOTICE` absent from it); the scan reduces each value
   to its verdict token before classifying and carries **no** `TOKEN*` prefix glob anywhere; the positive
   arm exists and its `*)` sets `failed=1`; all six deterministic keys are named in the backstop; and the
   backstop carries **exactly one** exempting arm;
9. **the removal itself, pinned** — the deleted key is absent from the verdict-scan key list, from the
   emit line and from `--help`, and the deleted functions are absent by name. A removal that is not pinned
   is a removal that comes back.

**What is deliberately NOT here (#3283):** no case asserts what roborev's exclusion set *would* do to a
given census, because no code predicts it. The fixtures therefore no longer supply a `.roborev.toml` at all
— nothing reads one, and an inert input that reads as load-bearing is the same class of misleading test
(D2b, lesson 3).

**Negative control on the retained surface**, because a suite that shrinks by ~26% and stays green proves
nothing unless something in it has been SEEN to fail: the retained closures and the path boundary were
mutation-tested in a scratch copy — reverting the exact-token match to prefix globs makes `cx28b`/`cx28c`
reach `RESULT: PASS` (RED), reintroducing a per-key exemption reds the structural assert, reintroducing a
`prompt-content:` subtraction reds its cases, and each path-boundary assert was individually verified to
FAIL under a deliberate mutation of its own fix. Restore green at **477/0**.

The suite runs under the `roborev-lints` gate component, which is in **both** `COMPONENTS` and
`LITE_COMPONENTS` — so a regression FAILs the fast loop rather than costing a review round. Its tally
line stays `GUARD-TEST RESULT: …`, distinct from every gate/wrapper verdict.

### D9 — doctrine, in the same change (AC6)

The false claim to be retired is "roborev **EXCLUDES non-code paths from the diff it builds**". It is
false in both halves: the exclusion is a **configured git pathspec set**, not a code/non-code judgement,
and under `docs/**` it excluded *code* — which is the whole issue. Doctrine is amended to state the true
mechanism, to name the `docs/reports/*-artifacts/` harness convention explicitly, and to stop implying
that everything under `docs/` is code-free.

**Doctrine also carries the hazard the deferred guard would have carried.** With no automated detector for a
re-broadening of `exclude_patterns`, the prose is the control: the doctrine surfaces and `.roborev.toml`'s
own comment state that `docs/**` must not be reintroduced, that a trailing slash inverts to recursive, and
that a `roborev config set` write can rewrite the file. That is weaker than a check and it is said so
plainly, here and in the proposal.

Every surface that repeats the retired claim is corrected in the same change, or the doctrine drifts against
itself: `CLAUDE.md` (roborev rule 4 and its T3 sentence, plus the docs-only/CITE-AND-WAIVE region),
`website/src/content/docs/agents-developing/roborev-findings.md` (rule 4 and its T3 paragraph),
`website/src/content/docs/agents-developing/delivery-pipeline.md`, `.claude/agents/flow-lead.md`,
`.claude/agents/flow-closer.md`, `.claude/skills/flow-implement/SKILL.md`, the header comments of all
three `scripts/flow/roborev-review*.sh` files (including the `roborev_check_prompt_content()` comment
that states the now-falsified claim outright), and this capability's own requirements in
`openspec/specs/roborev-review-guard/spec.md`.

The website deploys from `.github/workflows/docs-site.yml` (`Docs Site`, `deploy` job via
`peaceiris/actions-gh-pages@v4`); PRs build and link-check but do **not** deploy. So publication is
accepted **after merge**, by fetching the page and grepping for a distinctive phrase introduced by this
change — never by an HTTP 200, since the CDN can serve the previous page for ~3 minutes after a
successful deploy.

### D10 — the backfill ruling is recorded, and the decision stays the owner's (AC7) — RULED: accept as-is

AC7 asks for a *recorded decision* about the already-merged, never-reviewed harness code from #3026 /
#3100 / #3217, not for a particular decision. The requirement is therefore written as "the change
records the owner's ruling **and its reason**", with the scenario covering **both** branches — a
retroactive review pass or explicit acceptance-as-is with the reason stated. Leaving it unaddressed is the
only failing outcome.

#### THE RULING (owner, 2026-08-03): ACCEPT AS-IS. No retroactive review pass.

Recorded here in full, because an unrecorded "we decided it was fine" is indistinguishable from nobody
having looked. The reasoning, not merely the verdict:

1. **The exposure is BOUNDED by what the code is.** Every affected file is a *measurement harness* —
   a Part A/B driver, an off-CPU classifier, a demangler, a counter parser, a summarisation tool. None of
   it ships in the library, none of it is imported by `cqlite-core`/`cqlite-cli`/the bindings, and none of
   it runs in CI or the agent gate. A defect in it can corrupt a *report's numbers*; it cannot corrupt a
   release, a user's data, or a gate verdict. That is a materially different risk class from unreviewed
   library code, and it is the reason the decision can go this way at all.
2. **The largest tranche ALREADY had a full adversarial pass.** #3222's 34 executables were hand-reviewed
   file by file when the wrapper refused to certify them, and that review is recorded in the PR. It found
   **no blockers**, and it *did* find real defects — a 4th silent-failure instance where every driver log
   fabricated `rc=0` (`$(…)` resets `$?`) plus two provenance defects — all fixed before the PR merged. So
   the biggest slice of the exposure is not unreviewed; it is reviewed by a *more* expensive mechanism
   than roborev.
3. **The class is much less likely to recur silently, which is what a backfill would actually be buying.**
   The value of a retroactive pass is mostly the assurance that the *next* one will not slip through. That
   assurance now comes from the narrowed configuration — `exclude_patterns` no longer contains a blanket
   directory glob, so executables under `docs/` reach the reviewer — plus the `(cx*)` hermetic cases that
   fail the `--lite` loop if the census stops calling them code. It is **weaker than the ruling originally
   assumed**, because the pre-enqueue detector that would have caught a re-broadening is deferred to #3283;
   the honest statement is "the configuration is fixed and the consequence still fails closed under
   `prompt-content:`", not "the class cannot recur". A backfill still adds no part of that.
4. **Retroactively reviewing code whose outputs are already banked buys audit theatre, not safety.** The
   reports those harnesses produced are merged and have already been acted on. A finding now would not
   un-bank a number; it would produce a comment on a historical artifact. Spending review rounds — and
   a reviewer's attention — on that instead of on live code is a worse allocation, and pretending
   otherwise would be the dishonest part.

**What would change this ruling:** any of that harness code being promoted into a shipped path (a gate
component, a CI step, an imported module). At that moment it stops being a measurement artifact and
inherits the review obligation of the surface it joins. That is a rule about *promotion*, not about
history, and it is the standing follow-up this ruling leaves behind.

## Follow-ups (named here, deliberately not fixed here)

- **#3283 — the pre-enqueue exclusion-set guard (AC3 + AC4).** Deferred by owner ruling (D0). It inherits:
  the measured mechanism knowledge above; the constraint that R7 makes a port necessary unless upstream
  gains a way to print the resolved pathspecs; the requirement that any port carry a **differential**
  harness against the real binary (the NBSP lesson); the removed subsystem's four generalisable lessons
  (D2b); and the mirror assert D1a lost with the TOML parser.
- **#3278 — roborev's compiled-in built-in deny-list.** Separate, still open, still unmodelled (D2a).
- **`scripts/ci/classify-docs-only.sh` has the same defect in the correctness gate.** `is_docs_file()`
  classifies with a blanket `case "$path" in docs/*) return 0`, so a PR touching only
  `docs/reports/*-artifacts/*.sh` is classified **docs-only** and short-circuits `pr-gate-core` to green.
  Same shape — "a path glob swallows executables under `docs/`" — but in the gate that decides whether
  the code is *tested*, not merely reviewed. Its test is `scripts/tests/test_classify_docs_only.sh`. To be
  **filed as its own issue** during this change; entangling a review-gate config change with a
  correctness-gate change in one PR would make both harder to certify.
- **Upstream ask on roborev:** an allow-list / negation form for `exclude_patterns`; a way to print the
  resolved exclude pathspecs (which would retire the whole port-and-differential problem for #3283); and a
  non-zero exit when the constructed diff is empty because everything was excluded.
- **Deny-list drift watch:** a new artifact extension under `docs/` is re-admitted as review noise until
  the pattern list is extended (bounded, non-correctness — see D1).

## Doctrine compliance notes

- **No-heuristics mandate (#28):** unaffected. The mandate governs inferring on-disk TYPE/format from
  byte content in the SSTable read path. Nothing here infers anything: the change is a configuration
  narrowing plus a strictly stricter verdict scan.
- **Format authority:** not applicable — no on-disk format surface is touched.
- **Campsite rule:** the wrapper is at 1047 lines (over the ~800 target) and the oracles file at 721; the
  descope removed code from both. The 2784-line test file is over the ~1500 test target, so its growth is
  expected to be flagged and either split by responsibility or run with `CQLITE_ALLOW_FILE_GROWTH=1` and a
  note linking #1135.
- **Wiring evidence:** the public surface is `scripts/flow/roborev-review.sh` — the sanctioned
  invocation every flow-\* skill and agent calls. The end-to-end evidence is the recorded live probe
  (D6) plus the hermetic cases that drive the real wrapper end-to-end via its own summary block (D8).
