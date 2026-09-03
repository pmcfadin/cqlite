# Fleet Runbook — Running the Agentic Delivery Pipeline as the Owner

The exact, user-level version of how to run CQLite delivery across one or more machines.
This is the *operator's* doc — what you type, what you'll see, when the system needs you.
Doctrine and internals live elsewhere ([delivery pipeline](https://pmcfadin.github.io/cqlite/agents-developing/delivery-pipeline/),
`docs/development/pm-operating-loop.md`, `CLAUDE.md`); this page is the driver's seat.

> **Status:** the context-economy restructure (Tier-2 epic #2083, audit
> `docs/reports/agentic-workflow-audit-2026-07-06.md`) has landed — the disposable per-issue closer,
> inter-issue lead reset, claim heartbeats + deterministic reap, and the worker supervisor all work today.

---

## The model in one paragraph

**One Claude Code session per machine. N machines = N issues in flight.** Each session claims work
by acquiring the slugless fixed-name ref `refs/claims/issue-<N>` on origin (`scripts/flow/claim.sh`,
the cross-machine lock — #2665), works it in an isolated worktree, and merges its own PR when the
quality bar is met (gate PASS + roborev clean, + spec-audit PASS for design work). You touch the
system in exactly **two places**: approving specs (Seam 1) and making product calls (the NEEDS-YOU
list). Everything else runs itself.

The machine you're sitting at runs the **lead** (your conversation partner, which also works
issues itself). Other machines run pure **workers**. A lead is a worker with a human attached.

---

## Machine setup (once per machine)

```bash
git clone https://github.com/pmcfadin/cqlite && cd cqlite
bash scripts/bootstrap-agent-machine.sh        # or manually: sccache, cargo-nextest, bash>=4.3, mold (Linux)
bash test-data/scripts/fetch-datasets.sh       # real SSTable binaries — REQUIRED; export the line it prints
gh auth setup-git                              # git push credentials — SEPARATE from gh auth (#2942)
gh auth status                                  # must include the 'project' scope (board access)
```

**On a LAUNCHER-ONBOARDED box the credential step is already done (#3369)** — the agent-ami
profile's `verify.run` is
`bootstrap-agent-machine.sh --fix-credentials --fix-gate-pin --strict`, which runs
`gh auth setup-git` itself after the token is injected **and persists the single-gate pin**
(`--fix-gate-pin`, #3414 — without it a launched box arrives unpinned and every gate on it resolves
the #1825 cap from the default formula). It is **wired at onboard, not baked into the image**, so a
hand-built box still needs the line above; the two paths do not conflict, and re-running it is
idempotent.

**The `refs/claims/*` preflight is no longer a separate manual step either.** Every bootstrap run
MEASURES push capability by performing the operation — it invokes `claim.sh smoke` and reports one
of `git-push: VERIFIED` / `FAILED` / `UNMEASURED`. Run the probe by hand only when diagnosing:

```bash
bash scripts/flow/claim.sh smoke               # same probe the bootstrap runs (see below)
```

**The single-gate pin is provisioned and VERIFIED the same way (#3414).** Bootstrap persists
`CQLITE_GATE_MAX_CONCURRENCY=1` into **`/etc/environment`** under `--yes` — read by PAM's
`pam_env` at session creation, with no interactivity guard — and then reports one
`gate-pin:` line taken from an **affirmative probe**: it scrubs its own inherited value —
**and `BASH_ENV`/`ENV` with it**, since a non-interactive bash sources `$BASH_ENV` and that file
could re-export the very variable just removed — then reads the variable back out of a fresh,
profile-free session. It is never a grep of the file it just wrote. Five verdicts ship, only the
first an `[ok]`: **`VERIFIED`** (the file sets a value, the session sees THAT SAME value, and the
gate honours it), **`NOT-SYSTEM-WIDE`** (the session sees a value the file does not set — a sudo-
or user-specific override), **`NOT-HONOURED`** (visible, but the gate discards or clamps it),
**`FAILED`** (not visible), **`UNMEASURED`** (the probe could not run, the gate could not be
consulted, or the file could not be read/parsed), **`OPT-OUT`/`SKIPPED`**. `VERIFIED` is the ONLY `[ok]`.

On a **non-Linux** host there is no `[ok]`. `/etc/environment` + `pam_env` is a Linux mechanism, so
macOS is scoped out rather than supported (no launchd equivalent is shipped — there is no Mac on
this fleet to verify one against). An earlier form reported `NOT-APPLICABLE` as a second `[ok]`
here; that was right about the mechanism and wrong about the verdict, because `--strict` reads the
`[ok]` and so **certified an unpinned host**. Such a host now reports **`UNMEASURED`** when a value
is visible — with no system-wide file to correlate against, a machine-wide pin cannot be told apart
from a user-scoped one — and the per-run authority is the gate's own `cpu-budget:` token.
`NOT-APPLICABLE` is emitted nowhere in the script today.
Three facts to keep in mind when you touch this:

* **`VERIFIED` is SCOPED, and the line says so.** The probe measures a PAM-created (sudo)
  session, because that is the only session an unprivileged process can create. A gate is not
  launched through sudo: a supervisor or lane tree started from a systemd unit or a container
  entrypoint has no PAM in its ancestry, so `/etc/environment` never applies to it. `VERIFIED`
  therefore means "a PAM-created session here sees a value the gate honours", never "every gate
  on this box is pinned" — the authoritative per-run confirmation is that gate's own
  `cpu-budget: max-concurrency=N(pinned)` token.

* **A shell profile is the wrong place and a grep of it is the wrong check.** Stock Ubuntu
  `~/.bashrc` opens with `case $- in *i*) ;; *) return;; esac`, so an export appended there is
  never reached by the non-interactive shells that launch gates. All three fleet boxes carried
  the export and **none** of them had it in effect; every gate resolved the #1825 cap from the
  default formula (`--slots 3` on a 16-core box) while the pin looked installed. Bootstrap still
  appends to the profile for interactive convenience, but that append can only ever print
  `[info]`.
* **An existing value is never rewritten.** A box deliberately running >1 concurrent gate
  overrides the pin; bootstrap leaves the line exactly as it is and verifies effectiveness.

```bash
# how to ask the question yourself. The scrub and the '-' (NOT ':-') are load-bearing:
# without the scrub an INHERITED value reads as a healthy pin, and ':-' collapses "unset"
# with "set but empty" — the exact defect this issue removed from the gate's own resolver.
env -u CQLITE_GATE_MAX_CONCURRENCY -u BASH_ENV -u ENV \
  sudo -u "$(id -un)" bash -c 'printf "[%s]\n" "${CQLITE_GATE_MAX_CONCURRENCY-UNSET}"'
# and what the gate then stamps on its own cpu-budget line:
#   max-concurrency=1(pinned)   <- provisioned    max-concurrency=3(default) <- NOT provisioned
```

**Cost and residual of the automatic probe.** Every bootstrap run — laptop runs included — makes two
extra network round trips and CREATES AND DELETES a transient `refs/claims/smoke-<commit-sha>` ref on the
shared origin. A cleanup delete that exits nonzero now FAILS the probe
(`SMOKE-FAIL … reason=cleanup-unverified` → `git-push: FAILED`, #3369) rather than reporting success
with a stderr warning: delete capability is required by the claim protocol, since `claim.sh release`
deletes `refs/claims/issue-<N>`. That verdict deliberately attributes **no cause** — one nonzero exit
cannot tell a remote's deletion policy from a network drop — so it reports the observation and tells
you how to check. A ref can still be stranded two ways: by that case (the delete did not succeed, so
the ref's fate is unknown), and by a run KILLED between the create and the delete, which leaves no
verdict at all. **Deleting a stray `refs/claims/smoke-*` is always
safe** — it is a throwaway root commit that nothing reads, and it is NOT a claim lock (those are
`refs/claims/issue-<N>`, never `smoke-`). List and clean them:

```bash
git ls-remote origin 'refs/claims/smoke-*'
git push origin --delete refs/claims/smoke-<commit-sha>
```

### Notification channel (ntfy) — one env var, no per-machine binary (#3119)

Gate-completion pushes (#2667) and every supervisor page go over ntfy. **The payload contract lives in
the repo** (`scripts/lib/gate-notify.sh`), so the only thing a machine supplies is the transport and a
target:

```bash
# /etc/environment (fleet mechanism; re-login to pick it up)
CODEX_NOTIFY_WEBHOOK=https://ntfy.sh/<your-topic>
```

`bash scripts/bootstrap-agent-machine.sh` prints the section *Notification channel (ntfy, issue #3119)*:
it checks `curl` + `python3`, reports the target, runs `gate-notify.sh --self-test` as a **capability**
assert (it publishes a PASS *and* a FAIL payload through a private capture shim — no network, no real
topic) and records the pinned contract version. **No `agent-notify` install is required**, and no
hand-patched copy is required by anything in this repo. If `agent-notify` happens to be installed it is
used only as an optional local desktop/sound adjunct, invoked positionally with its webhook env
neutralized.

Why this is spelled out: the old path called `agent-notify --category <cat> …` (notify-flag-allow), a
flag upstream v1.1.0 has no arm for. It fell through to manual title/message mode, so the title became
the literal `--category`, the message became the category value, and **every FAIL paged as a green
priority-3 success**; its ntfy publish also POSTed to the topic URL, so phones rendered raw JSON. A red
gate that looks green is worse than no notification. With no target configured everything here is a
silent no-op and nothing else changes — notifications never affect a gate verdict or a worker's exit.

**Three deltas that fail with a message pointing away from their cause (#2942).** Each cost a
worker a diagnosis round-trip on a Linux box; the bootstrap now checks the first two and fails
loudly rather than reporting a healthy machine. Search for the message you actually saw:

- **`fatal: could not read Username for 'https://github.com'`** — `gh` is authenticated but **git
  is not**. They are separate credential paths, and `scripts/flow/claim.sh` +
  `scripts/flow/claim-heartbeat.sh` push with plain `git` on 10+ call sites, so the claim protocol
  — the cross-machine lock — simply does not work while `gh auth status` reports a happy machine.
  Fix: `gh auth setup-git`, or `bash scripts/bootstrap-agent-machine.sh --yes`, which configures a
  helper **scoped to the origin host** that dereferences `$GH_TOKEN` **at call time** (the token
  itself is never written to disk, so rotating it needs no reconfiguration). Because that helper
  reads the environment, it only works in shells where `GH_TOKEN` is exported — for an unattended
  systemd/cron worker prefer `gh auth setup-git`. Related: an unauthenticated push now reports
  `CLAIM: ERROR reason=auth … (NOT retryable …)` — it used to say `reason=infra … (transient —
  retry)` and send workers into a retry loop on a fault that can never self-clear. That
  classification covers `claim.sh` (`claim`/`adopt`/`release`/`smoke`) only; `claim-heartbeat.sh`
  surfaces git's raw error on its own pushes and does not classify them.
- **A `gh project` failure citing a missing `read:org` scope on a token whose scopes DO include
  `project`** — a scope match is evidence about a token, not about the operation. `gh project
  item-edit` needs `read:org`; the `updateProjectV2ItemFieldValue` GraphQL mutation does **not**
  and succeeds with the same token. Fix: `gh auth refresh -s read:org`, or route board WRITES
  through that mutation (which is what `project-board-sync.yml` already does). The bootstrap's
  board check is now a read-only functional probe for exactly this reason — it can no longer print
  "board dispatch works" on the strength of the scope string. It also reads scopes from the
  **active account's** stanza only and probes as `CQLITE_PROJECT_ACCOUNT` (the account `flow-board`
  forces active), restoring your previous account afterwards: `gh auth status` prints one stanza per
  logged-in account, so a whole-output grep can describe an account your commands never use — the
  EMU-account flip documented in `.claude/skills/flow-board/SKILL.md`.
- **`stale info` from a bare `git push --force-with-lease`**, even when local and remote refs
  demonstrably match — the bare form leases against a remote-tracking ref this checkout may never
  have fetched. Always use the explicit CAS form `git push --force-with-lease=<ref>:<sha>`, which
  is what the flow scripts already do; the bare form only bites a human or agent typing it ad hoc.

**Linux workers — mold linker (#2859):** on Linux hosts bootstrap also provisions the **mold**
linker (linking is the one build cost sccache cannot cache — every `--lite` round and full gate
re-links every test binary) and wires it through a managed block in the **per-machine**
`~/.cargo/config.toml`, after a link probe proves the toolchain accepts `-fuse-ld=mold`. It is
advisory (a missing mold never fails a run) and never touches the repo-committed
`.cargo/config.toml`. The gate's `accelerators:` line stamps `mold=linked` / `overridden` /
`present-unconfigured` / `absent` on Linux; if you see `present-unconfigured` (mold installed but
not wired), re-run `bash scripts/bootstrap-agent-machine.sh`. macOS is out of scope (mold is
Linux-only; ld-prime is already fastest there) — no change and no token. **Never export a global
`RUSTFLAGS` on a worker:** env `RUSTFLAGS` suppresses cargo's `target.rustflags` entirely, so the
wired `-fuse-ld=mold` goes silently inert — the gate stamps `mold=overridden` to surface exactly
this footgun; scope any `RUSTFLAGS` per-command instead. **One-time cold rebuild at enablement:**
adding the mold `rustflags` changes sccache's cache keys, so the first gate after mold is wired is
a **cold rebuild** (no cache hits) — expected, one time per machine; subsequent runs are warm
again.

**Claim-ref preflight (#2665):** the cross-machine lock is a push to the `refs/claims/*` ref
namespace on origin — `claim.sh smoke` creates, `ls-remote`s, and deletes a throwaway
`refs/claims/smoke-<commit-sha>` ref to confirm the remote permits it (`SMOKE-OK` = good). This is
**verified working on github.com/pmcfadin/cqlite** (2026-07-17). Since #3369
`bootstrap-agent-machine.sh` runs this probe on every invocation (that is its `git-push:` line), so
you rarely need it by hand; run it directly **when adopting a new remote or host** — a managed Git host that restricts custom ref namespaces would make the whole
claim mechanism unusable, and that must be caught before the fleet relies on it. **Non-unique
hostnames:** the claim holder identity is `hostname -s`; on a fleet of cloud images/containers/cloned
VMs that report the *same* short hostname, export a UNIQUE `CLAIM_MACHINE` per box (else two machines
share one identity and each treats the other's claim as its own).

Sanity check: `bash scripts/agent-gate.sh --lite` should pass in ~1–5 min **on a NARROW diff** (measured
median 1.4 min) — that expectation holds for that case only, so **a slow `--lite` is not by itself evidence of
a broken box (#3764)**: a diff touching `cqlite-core/src/` measures median 20 min (up to 43 min locally; ~104
min under peer load is reported, #3764), and a cold `clippy` alone adds 16–24 min whatever the diff. CLAUDE.md's Lite row carries
the full cost model. Whatever the diff, the SUMMARY's
`accelerators:` line should read `sccache=on nextest=on lanes=parallel`. If anything says
`absent`, the gate prints the one-line install fix — do it; a degraded machine is ~3× slower.

**Datasets matter:** without them, parity components skip. The FULL gate FAILs CLOSED when the
fetched validation corpus is absent (**#2078**), stamping `missing-fixtures: FAIL-CLOSED (#2078)`;
`AGENT_GATE_ALLOW_MISSING_FIXTURES=1` opts out visibly, and `--lite`/`--only` stay lenient. Fetch
them on every machine, day one — and export the `CQLITE_DATASETS_ROOT=` line the fetch prints, since
on a box with a machine-local root (e.g. `/data/datasets`) the checkout's `test-data/datasets` stays
corpus-less (**#3131**).

**The corpus root is all you need (#3131/#3148).** `CQLITE_DATASETS_ROOT` alone is sufficient on every
layout: the committed CQL schemas (`test-data/schemas`) are resolved **checkout-relative** and are not
a sibling of the corpus root. Do **not** create a `schemas` symlink next to a relocated corpus, and do
not assemble a composite root to make one appear — both are retired workarounds. The FULL gate has a
second fail-closed fixture cause for this half, `missing-schemas: FAIL-CLOSED (#3148)`, which fires on
an unreadable committed `.cql` **or** on a rejected relative `CQLITE_SCHEMAS_ROOT` (that override must
be absolute). It has **no opt-out** — committed source in a checkout is never legitimately absent.
`--lite`/`--only` stay lenient.

### Perf profiling capability (`perf_event_paranoid`) — Linux workers (#3249)

Agent/worker images ship with `kernel.perf_event_paranoid = 4` — **all** unprivileged `perf` use
denied — and set it in **no** sysctl file. So every profiling run started from a hard `EACCES` whose
help text ("access limited") reads like a *capability* verdict when it is a *permission* verdict;
that alone cost two measurement cycles. A box left permissive by a hand-probe is no better: with no
drop-in it reverts on reboot/reprovision, i.e. the fleet was profileable only by accident.

`bash scripts/bootstrap-agent-machine.sh --yes` now installs a reboot-surviving drop-in:

```conf
# /etc/sysctl.d/99-cqlite-perf.conf   (whole file is managed; byte-compared on re-run)
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
```

**Why `-1` and not `1`.** `perf_event_paranoid` is **cumulative** — higher is *more* restrictive and
each level keeps the ones below it: `>= 3` (an extra level Debian/Ubuntu kernels carry) **disallow
all unprivileged perf event use**, `>= 2` no kernel profiling, `>= 1` **no CPU-wide event access**,
`>= 0` no raw tracepoints, `-1` **(almost) all events permitted**. That `>= 3` level is what makes
the images' shipped **`4`** deny *everything* — down to a plain `perf stat` — rather than only the
CPU-wide collection, which is why the first probe on a fresh box fails outright.
CQLite's measurement doctrine mandates per-CPU
collection (`perf stat -C <cpu>`), which is exactly what `>= 1` forbids — so `1` is not "almost
right", it is a hard denial. `0` is the bare minimum that works; `-1` additionally lifts the perf
mlock limit, avoiding `perf record` ring-buffer surprises. `kernel.kptr_restrict = 0` is a
**separate** control needed for kernel *symbol* resolution — without it kernel frames render as
unresolved addresses, a **silent attribution loss, not an error**.

**BPF collectors still need `sudo`.** A permissive `perf_event_paranoid` does **not** grant BPF map
creation, so `bpftrace`/`bcc` collectors remain root-only (the #3217 finding). This drop-in buys
unprivileged `perf stat`/`perf record`, nothing more.

**Security posture — read this before copying the file anywhere.** This is a **deliberate
loosening**, appropriate for **dedicated single-tenant measurement/agent boxes** (what the fleet is:
dedicated agent/measurement lanes, no other tenants, no untrusted logins). It **must not** be applied to shared
or multi-tenant hosts: unrestricted `perf_event_open` plus unmasked kernel pointers lets any local
user observe other users' execution and leak kernel addresses.

**Bootstrap verifies rather than assumes** — the whole point, since a bootstrap that silently leaves
a box unprofileable is the failure mode being fixed. The section probes `/proc` first (writing
nothing when the drop-in is already current), writes the managed file, applies it, **reads the values
back out of `/proc/sys/kernel`** (a `sysctl` write's return code proves nothing — a container or a
later-sorting drop-in can swallow it), and finishes by running the collection itself:
`perf stat -C 0 -e cycles -- sleep 0.1`, requiring exit 0 **and a non-zero cycle count**. An rc-0
`perf stat` printing `<not supported>` (or a virtualised PMU counting a flat 0) is reported as
**NOT verified**. No sudo, no `perf`, or an absent `/proc` control degrades to a `[warn]` plus the
exact remedy line — bootstrap stays advisory and always exits 0. When the drop-in is already current
the remedy is **apply-only** (`sysctl -q --system`, prefixed with `sudo` only where a `sudo` binary
exists), never a pointless re-write.

**"VERIFIED" requires BOTH facts — the `/proc` token *and* the functional pass.** The functional
result alone is never the verdict: a box whose `/proc` says `paranoid-2` or `kptr-restricted` would
otherwise print its own diagnosis *and* a reassuring "VERIFIED" in the same run, and the reassuring
line wins the reader's attention. A functional pass without a matching `/proc` verdict is labelled
**partial diagnostic information**, explicitly subordinate to `/proc`.

**If you run bootstrap under `sudo`, know what the functional check can and cannot prove.**
`perf_event_paranoid` restricts **unprivileged** users — **root bypasses it entirely**. So
`sudo bash scripts/bootstrap-agent-machine.sh` (a normal invocation, and the likeliest one, since
installing the drop-in needs root) would run `perf stat -C 0 -e cycles` *as root*, where it **succeeds
on a `paranoid=4` box on which every unprivileged agent process still gets `EACCES`** — a textbook
false verification of an unprofileable box. Bootstrap therefore **drops privilege for the probe** when
it can: `setpriv --reuid/--regid --clear-groups` (preferred), else `runuser -u <name>`, else
`sudo -n -u '#<uid>'` (sudo's numeric form), targeting `SUDO_UID`/`SUDO_GID` (the account that invoked
`sudo` — the one whose capability is actually in question) and falling back to `nobody` resolved from
the passwd database, never a hardcoded uid.

**The numeric ids are the target; the NAME is only used when passwd confirms it.** `SUDO_UID` and
`SUDO_USER` are independent environment strings, so `SUDO_UID=1000 SUDO_USER=root` — stale, hand-set
or simply inconsistent — would make a name-based `runuser -u root` run the probe **as root** while the
run reported a successful privilege drop: a false "VERIFIED" through the drop mechanism itself. A name
is therefore used only after `id -u`/`id -g` confirm it resolves to exactly the validated **non-zero**
uid/gid (and only if it is shell-token safe, since the prefix is word-split); otherwise it is
discarded and the numeric mechanisms carry the drop.

**An unknown identity is never treated as "unprivileged".** If `id -u` is missing, fails, or prints
something unparseable, the state is `identity-unknown` and the functional result is **not** evidence
either way — the run says the identity could not be determined and makes no capability claim (it does
not assert the probe ran as root either). Nothing substitutes a plausible uid.

The run says which identity it measured:
`DROPS PRIVILEGE (dropped:setpriv:uid=1000)`. When **no** mechanism or no unprivileged account exists
(`root-no-drop-mechanism` / `root-no-unprivileged-target`), the root result is labelled **not evidence
that an unprivileged process can profile this box** and never reported as verification — the `/proc`
token, which is identity-independent, stays the authority. Two operator consequences: install
`util-linux` (for `setpriv`) on any box you provision as root, and to check by hand as the agent
account use `sudo -u <agent-user> perf stat -C 0 -e cycles -- sleep 0.1` (or
`bash scripts/perf-capability.sh --verify-unpriv`, which does the drop itself and fails when the
result cannot be attributed to an unprivileged identity). Plain `--verify` measures **whoever runs
it**, so as root it answers a question nobody asked.

**The apply's exit code and the capability verdict are SEPARATE facts, reported separately.**
`sysctl --system` applies *every* drop-in on the box, so it can apply ours perfectly and still exit
non-zero because an unrelated pre-existing entry failed (a stale `/etc/sysctl.conf` line, a foreign
drop-in naming a knob this kernel lacks) — one such entry anywhere is enough. Bootstrap therefore
re-reads `/proc` after **every** attempted apply, whatever the rc, and prints the command's failure
as a fact about the *command*. So a box can legitimately show "`sysctl -q --system` exited 1"
**and** a good read-back on the same run; that is not a contradiction, and the verdict is always the
`/proc` line. (Gating the read-back on that rc is what used to print "nothing was applied" about a
box that had just become profileable.)

**`/etc/sysctl.conf` BEATS our drop-in — check it first when the value does not take.** Both
`sysctl --system` and `systemd-sysctl` apply `/etc/sysctl.conf` **after** every `sysctl.d` drop-in,
so a stale `kernel.perf_event_paranoid` there wins over `99-cqlite-perf.conf` no matter how the file
sorts. That is the single likeliest cause of "applied, still restricted", and bootstrap's diagnostics
name it explicitly.

#### The "silent revert" has a NAME: `/etc/sysctl.d/10-kernel-hardening.conf`

Three separate reports recorded that a hand-set `perf_event_paranoid`/`kptr_restrict` "silently
reverts" without ever identifying a cause (`docs/reports/ws0-3217-report.md:214`,
`ws3-3029-report.md:63`, `ws0-cassandra-baseline-2026-07-27.md:847`). The cause is a **stock Ubuntu
drop-in**, present on the fleet boxes:

```conf
# /etc/sysctl.d/10-kernel-hardening.conf   (ships with the image; NOT ours, do not delete)
kernel.kptr_restrict = 1
```

It is re-asserted at **every boot** and by **every `sysctl --system`** — including the one bootstrap
runs. So a hand `sysctl -w kernel.kptr_restrict=0` survives only until the next boot or the next
`--system`, which is exactly the "it reverts on its own" experience. Nothing is wrong with the box;
a second file is simply also setting the knob.

**This makes the `99-` prefix LOAD-BEARING, not cosmetic.** `sysctl.d` files are applied in
lexicographic **basename** order and the **last** assignment wins, so `99-cqlite-perf.conf` beats
`10-kernel-hardening.conf` *only because of the number*. Renaming ours to `cqlite-perf.conf` — or to
any prefix below `10-` — silently hands `kptr_restrict` back to the hardening drop-in at the next
boot, with no error anywhere. **Never "tidy" the filename.** The managed file's own header says so
too, so the warning travels with the bytes.

**Bootstrap names the competitor for you — across the WHOLE `sysctl --system` search path.**
Whenever the read-back shows a non-`ok` token, the diagnostics scan every location `sysctl --system`
and `systemd-sysctl` load and report each *other* file that sets
`perf_event_paranoid`/`kptr_restrict`, ranked by whether it actually wins. **The search path, in
descending name-masking precedence** (`sysctl(8)` SYSTEM FILE PRECEDENCE / `sysctl.d(5)`):

| # | location | notes |
|---|----------|-------|
| 1 | `/etc/sysctl.d/*.conf` | where our managed `99-cqlite-perf.conf` lives |
| 2 | `/run/sysctl.d/*.conf` | volatile; a package or unit can drop a file here at runtime |
| 3 | `/usr/local/lib/sysctl.d/*.conf` | |
| 4 | `/usr/lib/sysctl.d/*.conf` | vendor drop-ins |
| 5 | `/lib/sysctl.d/*.conf` | usually a symlink to #4 on merged-`/usr` systems |
| 6 | `/etc/sysctl.conf` | **applied after every drop-in — wins regardless of filename** |

Two *independent* rules decide the outcome, and the scan implements both:

- **Masking** — "once a file of a given filename is loaded, any file of the same name in subsequent
  directories is ignored". So `/etc/sysctl.d/50-x.conf` replaces `/usr/lib/sysctl.d/50-x.conf`
  outright, and the diagnostic deliberately does **not** name the masked copy: it is not in effect.
- **Ordering** — the surviving files are applied in lexicographic **basename** order regardless of
  which directory they came from, and the **last** assignment wins. (`/etc/sysctl.conf` is outside
  this contest — it runs after all of them.)

```text
[warn] OVERRIDE: /run/sysctl.d/99-zzz-late.conf also sets perf_event_paranoid/kptr_restrict and its
       name sorts AFTER 99-cqlite-perf.conf, so it is applied LAST and WINS — fix or rename that file
[warn] OVERRIDE: /etc/sysctl.conf also sets perf_event_paranoid/kptr_restrict and is applied AFTER
       every sysctl.d drop-in ... so it WINS regardless of our filename — fix that file
[info] competing file: /etc/sysctl.d/10-kernel-hardening.conf also sets
       perf_event_paranoid/kptr_restrict but sorts BEFORE 99-cqlite-perf.conf, so ours wins
```

A run with no competitor says so explicitly **and names the path it covered**, so a silent scan can
never be mistaken for no scan. Scanning only `/etc/sysctl.d` used to let a later-sorting file in
`/run/sysctl.d` or `/usr/lib/sysctl.d` override us while bootstrap reported *no competitor* — the
same "reverts and nobody knows why" mystery wearing a different directory.

Verify by hand, in the same order bootstrap does:

```bash
cat /proc/sys/kernel/perf_event_paranoid /proc/sys/kernel/kptr_restrict   # want -1 and 0
bash scripts/perf-capability.sh --token                                   # want: ok
bash scripts/perf-capability.sh --verify-unpriv   # want: cycles=<non-zero> identity=self-unprivileged
#   (as root, --verify-unpriv drops privilege first; plain --verify measures whoever runs it, and
#    root BYPASSES perf_event_paranoid, so a root `--verify` pass proves nothing about an agent)
# apply by hand (identical bytes to what bootstrap writes, so a later run is a no-op):
bash scripts/perf-capability.sh --drop-in | sudo tee /etc/sysctl.d/99-cqlite-perf.conf >/dev/null
sudo sysctl -q --system
# still restricted? the override is almost always here (applied AFTER the drop-ins):
grep -Hn 'perf_event_paranoid\|kptr_restrict' /etc/sysctl.conf /etc/sysctl.d/*.conf \
  /run/sysctl.d/*.conf /usr/local/lib/sysctl.d/*.conf /usr/lib/sysctl.d/*.conf /lib/sysctl.d/*.conf
```

**Never set `CQLITE_PERF_PROC_DIR` / `CQLITE_PERF_SYSCTL_DIR` / `CQLITE_PERF_SYSCTL_EXTRA_DIRS` /
`CQLITE_PERF_TEST_SANDBOX` in a shell.** They are test-only path
seams for `scripts/tests/test_perf_capability*.sh` and are **inert** unless `CQLITE_PERF_TEST_MODE=1`
is also set — which in turn refuses to run if a real `sudo`/`sysctl` is reachable. The privileged
destination is a hardcoded `/etc/sysctl.d` literal precisely so no exported variable can ever steer
a `sudo tee` at another file, and bootstrap fails closed (skips the section with a `[warn]`) if it
finds a seam set without the marker.

**Test mode has NO production fallback (it is enforced, not conventional).** Under
`CQLITE_PERF_TEST_MODE=1` the sandbox root and **both** path seams are **mandatory**. An unusable seam
is a loud refusal in the env guard *and* in the path resolvers, so an unsandboxed test-mode run
resolves no drop-in path at all and reads no `/proc`. The earlier shape fell back to the real
directories, which meant a root `--yes` run under the marker could `tee` the host's **real**
`/etc/sysctl.d/99-cqlite-perf.conf` — a test run mutating the machine. There is deliberately no opt-out.

**The rule is POSITIVE CONTAINMENT in one declared sandbox — not a list of forbidden places.** Four
review rounds each closed one more *spelling* of "the production directory": the raw path, then a
symlinked seam, then `..`, then `//etc` (POSIX leaves two leading slashes implementation-defined,
`pwd -P` may preserve them, and on Linux `//etc` **is** `/etc`). A denylist over path spellings cannot
be completed — `.`, `..`, symlinks, `//`, trailing slashes, bind mounts, `/proc/self/root/…` all name
the same directory — and scattered prohibitions also let a *new* seam consumer miss them (that was the
`CQLITE_PERF_SYSCTL_EXTRA_DIRS` defect). So test mode now takes **one** caller-declared sandbox root,
`CQLITE_PERF_TEST_SANDBOX`, and a seam is usable **iff it is strictly inside it** (resolved-path prefix
match with an explicit `/` boundary, so `/tmp/sandboxevil` is not inside `/tmp/sandbox`). Anything not
provably inside is refused — every spelling above, and every future one, for the same single reason.

- The **root itself must prove it is a sandbox**: absolute, canonically spelled, existing, and holding
  the stamp file `.cqlite-perf-sandbox`. So `CQLITE_PERF_TEST_SANDBOX=/etc` cannot make containment
  vacuous — the proof lives on the filesystem, where placing it already needs the privilege the guard
  protects.
- **Every** consumer routes through that one check: the env guard, the drop-in path a root `tee` is
  aimed at, the `/proc` stand-in, and every configuration read (the lower-precedence search-path
  entries, plus an optional `sysctl.conf` **file** entry, whose parent is canonicalized and the
  resulting file path validated). `test_perf_capability.sh` enforces that **structurally**: it
  enumerates from the source every function that dereferences a seam and fails if one does not reach
  the containment check, so a new entry point cannot silently skip it.
- Paths that **write** or that **read host configuration** canonicalize both sides (`cd -P` + `pwd -P`:
  no `realpath`/`readlink -f` dependency, correct on bash 3.2); an unenterable path resolves to nothing
  and is refused.

**Containment of a SPELLING is not containment of a DESTINATION, and that cost four more rounds (#3261).**
Positive containment closed the path spellings; four escapes remained, each about something the guard
authorizes *other than* a directory name. All four are now closed by the same discipline — validate the
destination, positively, fail closed:

- **The write TARGET, not just its directory.** A contained directory says nothing about where its
  *entries* point, and `tee <path>` opens `O_CREAT|O_TRUNC` and **follows a symlink**. So a symlink at
  `99-cqlite-perf.conf` inside a perfectly-contained directory aimed the privileged write anywhere on the
  box. Now: naming that target **refuses** when it is a symlink, the idempotency read never follows it
  (a link whose target holds the canonical bytes must not report "already current"), and the write is an
  **atomic directory-entry replacement** — an **`mktemp`-created, unpredictably-named** staging entry
  in the validated directory (a *predictable* staging name that is checked, cleared and only then
  opened by a privileged writer is the same race one level down — and a pid suffix is predictable
  too, so `mktemp`'s `O_EXCL` create is the point, not the suffix), then `rename` over
  the name — so a symlink planted between the check and the write is *replaced*, not written through.
  Two further properties, both learned the hard way one level deeper:
  **(a)** the rename carries **`-T`/`--no-target-directory`**. Without it a **symlink-to-directory** at
  the managed name makes `mv` move the staging file *into* that directory — the rename that exists to
  avoid *following* a symlink follows one instead. Reproduced: the staging entry landed inside the
  outside directory, i.e. the managed bytes left the sandbox under a name nothing tracks.
  **(b)** the install **refuses a drop-in directory writable by anyone less privileged than the
  writer** — it must be owned by the identity doing the privileged write and be neither group- nor
  world-writable, with undeterminable owner/mode a refusal. *This* is what closes the staging race,
  and it took three review rounds to get there because the first two answers tried to make the race
  unwinnable instead of removing the racer.

  **Two false rationales were published here before that landed, and both are recorded rather than
  quietly deleted, because the failure mode is the interesting part.** First: a fixed staging name was
  called safe because the race "cannot happen" — it could. Then `mktemp` made the name unpredictable
  (closing the *create* race) and the remaining window was declared closed by putting every step in
  **one privileged `sh -c`**, on the stated grounds that no unprivileged process is scheduled between
  them. **That is false.** A single `sh -c` gives *sequencing within one process*; it is not mutual
  exclusion against other processes, which run concurrently on other CPUs entirely unaffected by how
  we grouped our own commands. Consolidation is retained — it is the right shape and removes needless
  windows — but it is **not** what makes this safe. The lesson generalises past this function: *"the
  attacker has no time to act"* is a claim about a scheduler you do not control, whereas *"the attacker
  cannot write this directory"* is a checkable property of the filesystem. Prefer the second shape.
  A non-shell helper holding the descriptor from creation to rename remains available but is now
  probably unnecessary rather than merely deferred.
- **A contained path can still SERIALIZE into two paths.** Not a containment defect — the path *is*
  contained — which is why nine rounds of containment work never saw it. The search path is emitted
  **one entry per line** and read back line-wise, so a directory legitimately inside the sandbox but
  *named* with an embedded newline (`<root>/evil\n/etc/sysctl.d`) splits into **two** entries, the
  second being the host's real `/etc/sysctl.d`. CR and LF are therefore rejected in every path seam at
  the boundary, inside the single containment predicate. This is the same class CLAUDE.md records for
  the roborev guard's own `-z` invariant: **a newline-delimited path set is not a safe representation
  of paths.** If you add a path-carrying seam anywhere, ask what happens when the path contains a
  newline before you ask whether it is contained.
- **The fork-free read path is NOT exempt** — the earlier claim here that a syntactic check was "sound
  because nothing there writes" was **wrong**, and this is what falsified it. A symlink *inside* the
  sandbox pointing at the real `/proc/sys/kernel` satisfies containment, so the run reported a token
  derived from the **host's real controls** while claiming to read a stand-in (measured: `paranoid-4`
  straight out of the live `/proc`). A **fabricated verdict is worse than a refusal**. The path stays
  fork-free and now rejects a **symlinked component** with builtins only (`[ -L ]` forks nothing).
- **A strictly-contained file must be ACCEPTED.** `<root>/sysctl.conf` was refused, because the check
  asked whether the *parent* was strictly contained and a root is not strictly inside itself. The judged
  path is now `<canonical parent>/<basename>`. A guard that refuses legitimate input is the guard people
  learn to route around.
- **EXECUTABLES too, not only paths.** `CQLITE_PERF_TEST_PRIV_DIR` was trusted textually: `/usr` is
  absolute and genuinely *contains* `/usr/bin/sudo`, and a symlink to the real `sudo`/`sysctl` inside a
  genuine shim dir is spelled locally while resolving to the host's binary — either one let a privileged
  test-mode bootstrap run a real `sysctl --system` against the host kernel. Every privileged executable's
  **resolved destination** must now be contained beneath the proven sandbox root, and `sudo`/`sysctl`
  **parked** in the shim dir are swept whether or not `PATH` reaches them (one `PATH`-order change is all
  that separates "not resolved" from "executed"). The structural audit covers privilege-shim resolution
  as a **second pass**, with its own floor and its own named allowlist.

Every gate SUMMARY's `accelerators:` line stamps the same state as a Linux-only `perf=` token
(`ok` / `paranoid-<N>` / `kptr-restricted` / `absent` / `unknown`), so "this box cannot be profiled"
is visible in any pasted block instead of being discovered at the start of a measurement cycle.
`paranoid-4` on a box you expected to profile means **re-run bootstrap**, not "perf is unavailable".

---

## Laptop A — your lead session (lead + worker)

```bash
claude --agent flow-lead
```

It orients from the board automatically and opens with something like:

> 3 Ready · 1 In Progress (laptop-B, #2081, heartbeat 4m) · NEEDS-YOU: spec approval #2084, product call on binding-parity compression.

### Your morning routine (~15 minutes, highest-leverage thing you do all day)

1. **Approve queued specs.** Say `activate <N>` for anything groomed, or just approve what's
   rendered. The lead shows the full spec + design inline — read it, then say **"approved"** (or
   redline it). *Why this is the routine:* design work waits a median **91 hours** for this
   moment; batching approvals here removes more wall-clock than any machinery change.
2. **Clear the NEEDS-YOU list.** Product calls, scope questions, epic closes. One at a time,
   recommendations first.
3. **Promote work.** Groomed issues land at board `Backlog` (groomed ≠ scheduled). Say
   **"promote #2078 #2079 #2081"** to fill the Ready column. An empty Ready column = the fleet
   idles, by design.

### Then let it work

Say **"work the queue"** (or just `implement <N>`). The lead claims an issue exactly like any
worker — branch push, assignee, board `In Progress` — and drives it through subagents. You can
interrupt at any time to groom an idea, ask "where do things stand", or approve a spec; the heavy
lifting is in subagents and background gates, not your conversation. The gate → review → merge
endgame runs in a disposable "closer" agent and the lead resets between issues (issues #2084/#2085),
so the session stays crisp all day — restarting it is also always free (state lives on the board and
disk, never in the window).

If you want Laptop A undistracted for a strategy session: **"hold claims — B has the queue."**

---

## Laptop B (and C, D…) — pure workers

```bash
cd cqlite && claude
```

then type:

```
/worker
```

That's it. The worker claims the top Ready item via the claim protocol, runs the full loop
(implement → lite gate each round → review → full gate ONCE → roborev → PR → **merges its own PR
on green** → finalize → telemetry stamp), then claims the next. It never needs your eyes
mid-issue. Leave it running.

**Hard rule: one session per machine.** Never start a second lead/worker session on a box that
has one — two sessions collide on worktrees and oversubscribe the CPU (SIGKILLed gates, flaked
perf tests, corrupted sccache under load — all field-observed). More throughput = another
*machine*, not another session.

### Overnight / unattended operation

Never leave one session grinding all night — context accretes across issues until the worker
degrades, and a session can't judge its own degradation from the inside. Instead, run the
**worker supervisor**: every issue gets a brand-new process (context hard-bounded at one issue),
and the supervisor — not a bare loop — guards the machine:

```bash
bash scripts/local/worker-supervisor.sh          # defaults: MAX_ISSUES=4, 8h ceiling
```

Each iteration spawns a headless worker with the validated invocation (issue #2841):

```bash
claude -p --output-format stream-json --verbose --dangerously-skip-permissions --agent flow-lead '/worker'
```

`-p` runs the prompt to completion and exits (no interactive TUI to block on); `--agent flow-lead`
is the registered orchestrator (`worker` is a `/`-command/skill, **not** an agent — `--agent worker`
exits 1); `--dangerously-skip-permissions` lets an unattended session run `gh`/`git` without a human
approving each prompt; `--output-format stream-json --verbose` streams the worker's live activity to
stdout so the supervisor's per-iteration redirect captures it (see monitoring, below). Override the
whole command with `WORKER_CMD` if needed; the default is what the supervisor uses when you don't.

What it guarantees:

- **One issue per session**: each iteration rehydrates from the board, resumes this machine's own
  claim branch first (crash recovery), else claims the next Ready item, works it to merged +
  finalized, and exits. Empty Ready = cheap no-op + backoff.
- **It cannot overload the box**: preflight holds the next iteration while load is high, a dead
  iteration's cargo/gate processes **or an orphaned worker Claude CLI** (the unattended
  `claude -p … --agent flow-lead` spawn shape, #2670/#2841)
  linger, or disk is low — it waits, it never spins. A **per-LANE** lock makes a second supervisor in the same
  **lane** refuse to start, while leaving other lanes on the box free (per-machine until #3393 retracted
  one-worker-per-machine; the default lock path is scoped to the lane's checkout root). **While the
  fleet's checkouts are mid-upgrade, EVERY supervisor start also checks whether the PRE-#3467
  machine-global lock `${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock` EXISTS (#3549)**, because the two
  paths are invisible to each other and a supervisor from an older checkout would otherwise co-run in
  the same worktree. **There is no opt-out — see "no way to skip it", below.**
  **The guard DETECTS that path and stops there — it does not open, read, enumerate or inspect it.**
  It answers exactly one question, in three values:
  - **present** (anything at that name, including a symlink, including a dangling one) → the start
    REFUSES with a `LEGACY GLOBAL supervisor lock` message (textually distinct from the per-lane
    "another instance is already running"). Remedy: stop that supervisor, or upgrade its checkout past
    #3467.
  - **verified-absent** → the start proceeds, **and it says so** — one `[worker-supervisor]`-prefixed
    line reading
    `legacy-lock check: nothing at <path>, which is the ONLY path this check tested …`,
    naming the resolved path. (The quoted fragment is deliberately verbatim: the suite greps this file
    for it, so rewording the emitted line reds a test instead of silently stranding this prose.) Read
    it as the narrow statement it is — see **the check's reach**, below — not as "no pre-#3467
    supervisor can be holding a lock on this box".
  - **could-not-tell** → the start REFUSES, and the cause says **THE EXISTENCE PROBE FAILED**. That is
    not a report that a legacy lock exists; it is a report that this run could not decide whether one
    does. Today the one reachable cause is a container (`$TMPDIR`) that is missing or not searchable by
    this user — remedy: make it exist and `chmod +x` it, then re-run.

  **THE CHECK'S REACH — one path, and it is derived from the checking process's own `TMPDIR` (#3549,
  roborev job 222 F1).** This is a scope statement, not a caveat to skim. The guard stats exactly
  `${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock` **as resolved by the environment of the supervisor
  doing the checking**, and nothing else, in any state. So a pre-#3467 supervisor launched with a
  **different `TMPDIR`** resolves its machine-global default to a different absolute path, and one
  launched with an **explicit `SUPERVISOR_LOCK`** (the pre-#3467 script honours that variable too, and
  there it names the machine-global lock) holds whatever path its own launcher chose. In both cases that
  lock is at a path this check never looks at, and a `verified-absent` is a true statement about the path
  it tested and says nothing about the one that supervisor holds. **Do not read a clean start as "there
  is no legacy supervisor here."**

  **Why that is not fixed, and why it is not going to be.** Another process's environment is unknowable
  from inside this one — we cannot read the `TMPDIR` a supervisor we have never seen was launched with,
  nor a lock path its launcher picked. The tempting reading, "fail closed when the path cannot be
  established", degenerates: the path can never be established for an arbitrary launcher, so
  fail-closed would mean refuse **always**, and a guard that never permits a start is broken rather than
  safe. Probing extra candidate paths was **rejected**, not merely skipped: the guard has no way to tell
  a stale lock from a live one (the classifier is deleted — see the paragraph above), so every extra
  probed path is one more place where a leftover directory refuses **every lane, permanently, with no
  remedy**. That inverts the trade the refusal is worth making at one canonical path an operator can
  reason about. So the scope is **declared** — in this paragraph, in the proceed-path line, in the
  refusal's own text, and in a RESIDUAL block at the guard — instead of being papered over with probes
  that would look complete.

  **Operationally, that means the guard has TWO residuals, one in time and one in space, and they retire
  together.** In **time**: a pre-#3467 supervisor that starts *after* the check is not stopped by it
  (#3596). In **space**: a pre-#3467 supervisor whose lock is not at the path we tested is not seen by
  it (this paragraph). Both close under the **same** condition as the guard's own deletion — every
  checkout a launcher can reach at or past #3467, at which point no pre-#3467 supervisor can run under
  any `TMPDIR` or lock name. When you check that condition by hand, note that
  `ls -d "${TMPDIR:-/tmp}"/cqlite-worker-supervisor.lock` answers for **the `TMPDIR` of the shell you
  run it in and no other**; the ancestry half (`git -C <checkout> merge-base --is-ancestor f33f726c4
  HEAD`, for every checkout a launcher can reach) is the half that actually closes it, because it holds
  whatever path a launcher would have picked.

  **Read what the refusal does and does not say, because this changed (#3549).** It names the path and
  it says a path exists there. It makes **NO claim** about what the object is, whether a holder is
  alive or dead, or whether removing it is safe — the guard did not look. Earlier versions classified
  the lock (`live <pid>` / `stale <pid>` / `unknown <cause>`) by parsing its `pid` file and measuring
  that pid's liveness, and printed a per-state remedy including a deletion one-liner. **All of it is
  deleted.** The reason is worth knowing, because it is why you now get less information: once the
  reclaim was removed, *every* state refused, so the classification could not change the decision —
  its only outputs were the wording and which remedy printed, while each of its parts (a pid parse, a
  platform pid bound, a NUL probe, a collation-free digit test, a wholesale neutralisation of the
  caller's inherited glob state) had absorbed a review round of its own. Machinery whose output cannot
  change the decision is not a guard.

  **The one printed command is READ-ONLY, and that is deliberate (#3549).** The refusal prints, on a
  line of its own, bare and complete, `ls -ldn -- <legacy path> && ls -lna -- <legacy path>` — the
  paths rendered in a **one-line** escaping form so the line is paste-safe and never wraps (a newline
  in `TMPDIR` would otherwise split it, and the diagnostic paths, across physical lines and leave
  prose fragments indistinguishable from the one bare command line), and with `--` because an
  option-shaped `TMPDIR` (`-scratch`) would otherwise be parsed as flags. Run it to see what is
  actually there; the guard has not verified anything about it beyond its existence. **No deletion
  one-liner is printed any more**, and the reason is measured, not stylistic: with a **symlink** at the
  legacy path pointing at a foreign directory, the old `rm -f -- <legacy>/pid && rmdir -- <legacy>`
  follows the link and **deletes that directory's `pid` file** (rc=0), after which the `rmdir` fails
  with "Not a directory" — so an operator destroys a file the guard never examined and the lock is
  still there. While the shape check existed it was what licensed printing a deletion; with no
  inspection there is no licence. If you decide to remove the path, that is your call once you have
  established that no pre-#3467 supervisor can run on the box, and **the ORDER is part of it**: stop or
  upgrade the legacy launcher **first**, remove **second** — removing first frees the legacy name for a
  pre-#3467 supervisor to take at once, which is the collision the guard refuses. Use a non-recursive
  removal (`rmdir` refuses a non-empty directory) so nothing you have not examined is deleted.

  **What the probe can and cannot distinguish, stated rather than implied (#3549).** Bash's file tests
  expose no errno, so `[[ -e X ]]` is false when `lstat(X)` fails for *any* reason. Decidable, and each
  is measured by the suite: a missing or unsearchable container (→ could-not-tell), an `lstat` of the
  child that succeeds (→ present, for every object type including a symlink), and a searchable
  container where the child reports ENOENT (→ verified-absent). **Not decidable, and therefore reported
  as verified-absent**: an `lstat` of the child that fails for a reason *other* than ENOENT (EIO on a
  failing disk, a stale network mount), and a divergence between `access(2)` — what `-x` asks — and
  actual traversal under SELinux/AppArmor or an NFS ACL. Closing the first would need an external
  command for the errno, which would put the probe's verdicts at the mercy of `PATH`; the gap is
  written down instead of papered over with a probe that looks complete.

  **This guard NEVER MUTATES the legacy lock, in any state**: it does not rename, delete, adopt,
  re-create — or now even read — it, so it cannot corrupt any holder's lock, live or dead. The reclaim
  it used to perform was removed because a reclaim must be able to RESTORE on its abort paths, and
  restoring a directory-with-contents is not atomic here — `mkdir` + `mv` leaves the lock observable
  *without* its `pid`, a window in which a pre-#3467 supervisor reads it as stale, reclaims it, and our
  restore then corrupts ITS lock. The atomic form (build the lock complete in a private staging dir,
  move it in ONE `rename(2)`) needs GNU-only `RENAME_NOREPLACE`/`mv -T` and this script supports macOS,
  so it was available and declined; the rationale is recorded in full at the guard.

  **There is NO WAY TO SKIP IT, and `SUPERVISOR_LOCK` is not one (#3549, lead ruling 2026-08-30).**
  The guard used to be skipped whenever you named the lock yourself, on the reasoning that you had
  taken the placement decision. **That exemption is removed as unsound**, and the proof is one
  sentence: an explicit `SUPERVISOR_LOCK` renames *our* lock, while a pre-#3467 supervisor uses the
  machine-global path **regardless** — it has never heard of the variable — so the skip switched the
  check off in exactly the case where the collision is still **live**. It conflated a naming choice
  with an isolation guarantee. `SUPERVISOR_LOCK` remains fully supported for what it legitimately
  does: **choosing where this lane's own lock lives**. It has no effect on this check, on any run.
  No refusal path mentions it either (one once did, in a generic remedy line printed by every state,
  which told an operator whose start had just been refused *because a legacy lock is there* how to
  start anyway — the exact collision the guard exists to prevent). The refusals offer exactly two
  remedies, and both are real: **stop the pre-#3467 supervisor, or upgrade that checkout to #3467+.**
  If you need a lane to start while that path is occupied and you have independently established that
  no pre-#3467 supervisor can run on the box, the action is to remove the path (order matters — see
  the read-only inspection line above), not to look for a variable.

  **It is a STARTUP check, not machine-global exclusion: it REDUCES the collision window, it does not
  eliminate it** — a pre-#3467 supervisor that starts *after* the check cannot be stopped without
  reimposing machine-global exclusion, which #3393 forbids (N lanes per box). That residual is tracked
  as **#3596** and recorded in a RESIDUAL block at the guard, beside the **spatial** residual described
  under "the check's reach" above; neither is the only one, and both are in that block.
  The guard is deletable once every checkout on the box is at or past #3467 — the condition is recorded
  at the guard in `scripts/local/worker-supervisor.sh`. (The Claude probe keys on the supervisor's own `-p … --agent flow-lead`
  spawn shape, so a legitimate interactive `claude` REPL or an interactive `claude --agent flow-lead`
  lead session — neither carries `-p` — is not matched.) A
  hold cannot latch it silently: every hold pass re-checks the stop-file and the wall-clock budget,
  and a leftover hold that never clears stops the loop loudly, paging the surviving PIDs (#2670).
  The two leftover families are bounded **separately** (#2670): a non-self-clearing orphaned worker
  CLI (`leftover-worker`) trips the tight `LEFTOVER_HOLD_MAX` (default 3 ≈ 15 min), while a
  self-clearing build/gate process (`leftover-build`: cargo/nextest/gate-slot-daemon) gets the loose
  `BUILD_HOLD_MAX` (default 12 ≈ 1 h, `<=0` disables) so a legitimate concurrent full gate (15–25 min)
  is waited out, never mistaken for a stuck orphan.
- **It cannot be fooled by a false finalize (#2670)**: a `finalized` marker is trusted only after
  the claimed PR gh-verifies as MERGED (via `state,mergedAt,autoMergeRequest`). A worker that parked
  its endgame yet wrote `finalized` is caught (`verified: mismatch:<state>`, confirmed across grace
  re-reads that absorb read-after-merge lag), paged high, judged abnormal, and never credited; a
  forged PR reference — non-numeric, a non-pmcfadin/cqlite URL, or one gh *resolves as absent* (gh's
  `could not resolve to a PullRequest` signature only — a transport `not found` like DNS/proxy 404 is
  **not** forgery) — is `mismatch:UNRESOLVED` (same escalation). An OPEN PR with **auto-merge armed**
  is the closer's legitimate path, judged `finalized-pending-automerge` (uncounted, breaker-neutral),
  not a false finalize. Such PRs are tracked **per-PR** (#2670): each is re-verified on later
  iterations and, once it reaches MERGED, **retroactively credited** toward `MAX_ISSUES`
  (`pending-credited`) — so a fast fleet with several *distinct* PRs pending at once is never mistaken
  for a stuck one. Only when the **same** PR is observed still-unmerged across `PENDING_AUTOMERGE_MAX`
  consecutive iterations **and** has been pending at least `PENDING_AUTOMERGE_MIN_SECS` (a wall-clock
  floor above CI time, so a burst of fast no-progress iterations can't burn the budget) is it
  auto-merge-stuck and the loop stops (`automerge-stuck`); a tracked PR that instead ends
  CLOSED-unmerged pages high (`pending-dropped`), never silently swallowed. A GitHub
  *outage* — or a missing JSON
  parser, a tooling gap that must never read as forgery — yields a neutral `finalized-unverified`
  (paged, uncounted, breaker untouched); a **persistent** outage is bounded: `UNVERIFIED_MAX`
  consecutive unverifiable finalizes stop the loop (`verify-unavailable`), so the `MAX_ISSUES` ceiling
  can't drift.
- **It cannot fail silently**: a push notification (ntfy) on every merge (info) and on any
  stop/hold/breaker-trip (alert). 2–3 consecutive abnormal exits trip the breaker → stop + alert,
  never hot-respawn. One journal line per iteration (issue, verdict, duration, PR, `verified`).
  **The payload is repo-owned (#3119)** — see *Notification channel* below; an `alert` publishes ntfy
  priority 5 + `rotating_light`, an `info` priority 3 + `white_check_mark`, so a red page can never
  look like a routine one.
- **It never wedges on a question (#2666)**: a worker that hits Seam 1 or a genuine owner decision
  **parks** (posts a `needs-decision` question comment + EXITs) rather than waiting — the supervisor
  judges it `parked-on-owner` and pages the owner once. A worker that nonetheless gets stuck on an
  interactive prompt is caught mid-iteration by a log-tail watchdog and paged as `stuck-on-question`.
  **Neither counts toward the crash breaker.** The watchdog reads the per-iteration capture at
  `$LOG_DIR/iter-<N>.log` — under `-p` a worker writes its narrative to the session transcript, not
  stdout, so the supervisor's default `WORKER_CMD` adds `--output-format stream-json --verbose`
  precisely so the redirect captures a live event stream; the watchdog's "prompt signature in the
  tail AND log size frozen across two scans" logic then works, and the log stays useful to a human
  (a wedged worker's byte size freezes exactly when the stream stops). **Watch a live worker** with
  `tail -f "$LOG_DIR/iter-$(ls -1 "$LOG_DIR" | grep -oE 'iter-[0-9]+' | sort -t- -k2 -n | tail -1 | cut -d- -f2).log"` (or simply `tail -f "$LOG_DIR"/iter-*.log`).

**Per-iteration verdicts** (one journal line each):

| Verdict | Meaning | Breaker |
|---------|---------|---------|
| `finalized` | claimed → gate/review → merge-on-green → finalized (`issue`+`pr` set) **and the PR gh-verifies as MERGED** (#2670); journal `verified: merged` | resets |
| `finalized-unverified` | well-formed finalize, but gh could not confirm the merge — gh missing / network / rate limit, **or no JSON parser present** (a tooling gap is never read as forgery, #2670); journal `verified: unverified`, default-priority page, **not counted** toward the issue budget | **neutral** (neither trips nor resets) |
| `finalized-pending-automerge` | PR is OPEN with auto-merge armed (the closer's auto-merge path, #2670) — it will land; journal `verified: pending-automerge`, default-priority page, **not counted yet**, tracked per-PR for retroactive credit; the **same** PR still-unmerged `PENDING_AUTOMERGE_MAX` iterations in a row ⇒ `automerge-stuck` stop | **neutral** |
| `pending-credited` | a previously `finalized-pending-automerge` PR re-verified as MERGED on a later iteration (#2670) — **retroactively counted** toward `MAX_ISSUES`; journal `verified: merged` | **neutral** |
| `pending-dropped` | a tracked armed PR that ended **CLOSED-unmerged** (auto-merge dropped / PR closed) on re-verification (#2670) — HIGH "armed PR did not land" page, dropped uncredited (never silently swallowed) | **neutral** |
| `no-work` | nothing Ready / nothing to resume — backoff, then retry | resets |
| `blocked` | stopped short of merge for an owner escalation; same issue twice ⇒ head-blocked stop | resets |
| `parked-on-owner` | clean park (#2666): `blocked` marker with `reason: seam1-approval\|needs-decision`; high page, loop advances | **never** |
| `stuck-on-question` | worker wedged on a prompt, detected mid-iteration; high page with the captured text | **never** |
| `abnormal` | nonzero exit / missing / malformed marker / unknown outcome / **finalized marker whose PR is a stable non-merged state** (`verified: mismatch:<state>`, after grace re-reads) **or a forged PR ref** (`verified: mismatch:UNRESOLVED` — non-numeric, foreign-host URL, or gh-unresolvable); high page naming the discrepancy (#2670) | **+1** |
- **It stops on its own**: at the issue budget or wall-clock ceiling — overnight is "clear a few
  issues safely," not "run unbounded."
- **Stop it yourself:** `touch .worker-stop` (finishes the current issue, then exits).

**Morning check:** your phone already told you the headline. On the lead: `what needs me` —
merged PRs, anything held/reaped, the NEEDS-YOU list. A stale heartbeat *plus* no alert received
= the supervisor itself died — the one unambiguous alarm.

Safe by construction: a worker session holds zero irreplaceable state (claim = origin ref,
code = worktree commits, criteria = issue body, verdict = summary file, next = board).

---

## What you'll be asked, and what you never will be

**You WILL be asked (the only interrupts):**
- **Seam 1:** "Here's the spec + design for #N — approve?" (design-driven work only)
- **NEEDS-YOU:** product decisions, scope/title changes, epic closes, genuine design-call review
  findings, `HOLD` conflicts. Always as a list with a recommendation.

**You will NEVER be asked to:** merge a green PR (workers merge their own), re-run a gate, read a
gate log (the ~15-line SUMMARY block is all anyone sees), or arbitrate a claim conflict (git's
server-side ref arbitration on `refs/claims/issue-<N>` decides every race — #2665). **History note:**
the earlier slug-named branch lock guaranteed only *same-name* atomicity — two sessions on
*different* slugs, or on an identical `origin/main` SHA, could both "win" (the #1632 slug pair; the
identical-SHA no-op "up-to-date" push). The "collisions impossible by construction / 0 in 174 issues"
claim overstated that: same-slug collisions were prevented, slug/SHA races were not. The fixed-name
claim ref (#2665) is what actually closes the class.

## Phrasebook

| You say | What happens |
|---|---|
| `groom <idea>` | One scoped issue, oracle/design routed, lands at Backlog |
| `promote #N #M` | Board Status → Ready; the fleet may now claim them |
| `activate <N>` | Worktree + OpenSpec; spec rendered for your approval (Seam 1) |
| `approved` | Implementation begins |
| `implement <N>` / `work the queue` | This session claims and drives it |
| `where do things stand` / `what needs me` | Board render + the single furthest-along item |
| `hold claims` | This session stops picking up new issues |
| `HOLD: merge #X after #Y` | Ordering constraint workers must obey |
| `finalize <N>` | (Rarely needed — workers self-finalize after merging) |

---

## Reading the board

`what needs me` on any lead shows: item · Status · assignee · priority · claim (`refs/claims/issue-<N>`) ·
machine + heartbeat age (issue #2089). Interpretation:

- **Ready, no claim ref** → next thing a worker will grab
- **In Progress, heartbeat fresh** → leave it alone
- **In Progress, heartbeat stale** → deterministically reaped by flow-board (heartbeat age > 4h AND no
  open PR → Status → Ready, work preserved on the branch, traceable comment; issue #2089). **On a
  SUPERVISOR fleet only**, the supervisor also stamps a lane-scoped claim ref
  `refs/lane-claims/<machine>/<issue>` (per-lane since #3393; the legacy per-machine ref is still read
  only so a pre-ruling one gets drained) that the
  `project-board-sync` 30-min cron's `reap-claims` job reaps on the SAME predicate server-side (age >
  4h AND no open PR AND, for a local claim, PID-dead) — so a supervisor that dies overnight gets its
  claim reaped by CI without waiting for a human to run flow-board (issue #2655). **`worker-supervisor.sh`
  is the only IN-TREE CALLER that CREATES OR REFRESHES `refs/lane-claims/*`**, so on this fleet — `/drive-issue` lanes, zero
  production supervisors — that namespace is EMPTY (measured on all three boxes) and neither the
  `reap-claims` job nor `dead-lanes` has a subject. What IS populated here is the per-issue lock
  `refs/claims/issue-<N>` and the per-MACHINE heartbeat `refs/heartbeats/<machine>`; see *Lane liveness on
  a supervisor-less `/drive-issue` fleet* below (#3548)
- **Ready but branch already on origin** → parked-by-design (e.g. spec approved, awaiting a
  team) — pickup is *resume that branch*, never a fresh claim. **This exact signal is AMBIGUOUS and the
  ambiguity is unresolved**: #3436 reads *Ready + pushed branch + no claim ref* as UNCLAIMED WORK (a
  session driving an issue it never claimed), and this bullet reads the same shape as benign. Neither
  reading is a lane-DEATH verdict; for what the two board signatures do and do not establish, read the
  one canonical statement in *Lane liveness on a supervisor-less `/drive-issue` fleet* below. Nothing
  mechanical tells these apart today — check the branch's last commit
  time and whether a session is actually driving it before deciding

## Recovery scenarios (all safe by construction)

| Scenario | What to do |
|---|---|
| Laptop lid closed mid-issue | Nothing. Commits are on the origin branch. Reopen and say `implement <N>` — it resumes from the worktree. |
| Session feels degraded / bloated | Kill it, start fresh. Board + disk are the state; the new session rehydrates in one board read. |
| Board unreachable (auth/scope error) | The session STOPS by design (labels are decorative, never a dispatch source). Fix `gh auth refresh -s project` and restart. If the scope is already present and `gh project` still fails for `read:org`, that is the #2942 delta — use the `updateProjectV2ItemFieldValue` GraphQL mutation for board writes, or widen the token with `-s read:org`. |
| `fatal: could not read Username` on any push | git has no credentials even though `gh` does (#2942). The claim protocol pushes with plain git, so it is fully broken until fixed: `gh auth setup-git`, or `bash scripts/bootstrap-agent-machine.sh --fix-credentials`. A claim attempt on such a box reports `reason=auth`, not a retryable transient. **Since #3369 the bootstrap catches this one step earlier**, by performing the push rather than probing the credential helper: the same fault shows there as `git-push: FAILED`. Never wait for a lane to hit it. |
| Gate seems hung | It's probably queued: look for `waiting for gate slot (N in use)…`. Queued ≠ hung. |
| Green SUMMARY but parity lines say SKIP | Datasets missing on that machine — `fetch-datasets.sh`, export the root it prints, re-run. Probe an existing root with `fetch-datasets.sh --verify-only` (mutates nothing). The FULL gate FAILs CLOSED here (`missing-fixtures: FAIL-CLOSED (#2078)`) so it can't slip through; `--lite`/`--only` stay lenient. |
| `missing-schemas: FAIL-CLOSED (#3148)` | Either a committed `test-data/schemas/*.cql` is unreadable (broken checkout — `git restore --source=HEAD -- test-data/schemas`) or `CQLITE_SCHEMAS_ROOT` is set to a **relative** path (export an absolute one, or unset it). Never a corpus-layout problem: the schemas root is checkout-relative. No opt-out exists — do not look for one. |
| Two machines want the same issue | Impossible past the claim: the second claim-ref push is rejected server-side (non-fast-forward on the fixed-name ref, #2665); the loser sees `CLAIM LOST` and picks the next Ready item. |
| **SSH accepts TCP but sends no banner** (from inside the VPC) | **Check `dmesg` for an OOM kill BEFORE concluding the instance is broken** — see the diagnostic order below. This is a memory symptom far more often than a broken box, and a soft reboot may be silently ignored. |
| **A new tmux session lands on claude's first-run login chooser** (so a retired lane cannot be replaced) | The credential did not reach the pane. `bash scripts/claude-auth-capability.sh --report` **observes** where it stops — it is a report and certifies nothing, so read the lines rather than an exit status. Repair with an explicit `bash scripts/bootstrap-agent-machine.sh --fix-claude-auth`, which **overwrites** the running server's credential with the persisted one and is **not** implied by `--yes`. **No browser, no re-login, no reboot.** Full mechanism below, "Claude credential reachability" (#3733). |
| A lane vanished — worktree clean, claim held, nothing reported | **On a `/drive-issue` fleet, `dead-lanes` is a DEAD END — read *Lane liveness on a supervisor-less `/drive-issue` fleet* below first (#3548).** Its subject set is `refs/lane-claims/*` (+ legacy `refs/machine-claims/*`), written by the supervisor (who writes them and who does not is stated once in *Lane liveness on a supervisor-less `/drive-issue` fleet*), and on this fleet it had nothing to report when measured — *nothing was reported*, never *nothing is dead*. The measurement, its date and its limits are stated once in the section below. What to do instead: check `dmesg` for an OOM kill (diagnostic order below), then reconcile the board against the branch using **the two board signatures, NEITHER of which is a verdict** — stated in full, once, in *Lane liveness on a supervisor-less `/drive-issue` fleet* below, and deliberately not restated here. **On a SUPERVISOR fleet** the command is the right tool: `bash scripts/flow/claim-heartbeat.sh dead-lanes` (#3393). Reports every claim whose owning process is gone, with no 4h wait and without suppressing a lane that holds an open PR. `should-reap` will not tell you: it consults the PID only after the claim is >4h old, so a lane killed a minute ago is indistinguishable from a healthy one for four hours. **Exit 3 = a dead lane was found; exit 1 = none was found.** This slice is positive-detection only and **never exits 0** (#3393 split): act on 3, never read 1 as a clean bill of health. Per-lane refs do make a sound clean verdict possible — a surviving lane's stamp no longer overwrites a dead sibling's — but it is tracked separately. LOCAL-ONLY: run it ON the box. A just-spawned lane reads `UNKNOWN-IDENTITY` until the supervisor's next stamp refresh; that is expected. |


### Lane liveness on a supervisor-less `/drive-issue` fleet (#3548)

**Lane-granular dead-lane detection does not run on this fleet, and we say so rather than pretend
otherwise** — owner ruling 2026-09-01 on #3548 (option C, descope and document; completes #3393).

**This section is the CANONICAL statement of the two board signatures.** Every other site — the
`dead-lanes` section of `claim-heartbeat.sh --help` (there is no subcommand-level help), the recovery
table above, `CLAUDE.md`, the website delivery-pipeline page —
carries at most a one-line summary and a pointer here. Five review rounds on #3548 (jobs 38, 40, 41,
47, 55) were all propagation failures of one duplicated statement, so the duplication was removed
rather than guarded. Edit the signatures HERE and nowhere else.

Why: `dead-lanes` enumerates `refs/lane-claims/<machine>/<lane-id>` plus the legacy
`refs/machine-claims/<machine>`, and **the only IN-TREE CALLER that CREATES OR REFRESHES either is
`scripts/local/worker-supervisor.sh`**. This fleet runs `/drive-issue` lanes, not supervisors.
**Measured on 2026-09-01, on all three boxes:** `lane-claims=0 machine-claims=0`, production
supervisors ZERO, while `claims=6 heartbeats=20` — so the detector had no subject and exited 1.
**That is a point-in-time measurement of THIS fleet, not a property of supervisor-less fleets in
general:** refs persist after supervisors stop, the legacy per-machine refs are deliberately still
read so pre-ruling ones drain, and `stamp` is a documented subcommand that can create a lane ref
directly — so a migrated, previously supervised or manually stamped fleet can legitimately produce
rows. The CI reaper (`project-board-sync.yml`, via `claim-heartbeat.sh reap`) also WRITES these
namespaces — it DELETES stale refs — which is why the precise relationship is "creates or
refreshes", not "is the only thing that touches them". The operational conclusion holds either way: **exit 1 means "nothing was reported", never a
clean bill of health.**

The two populated namespaces were **measured** and rejected as substitutes, so do not expect a
read-side "fix":

- `refs/claims/issue-<N>` carries a pid, but it is the **transient claiming shell's** and is never
  refreshed — measured dead (`pid=3775744`) while its lane was running. Reading it would report a
  dead lane for a healthy one.
- `refs/heartbeats/<machine>` is **single-slot per machine**, force-updated by `beat`, so N lanes on
  one box overwrite each other and at most one is ever reportable — structurally the same masking
  defect the retired per-machine claim ref had (instance 5 of the #3464
  retracted-invariant-in-a-second-carrier family).

**AC4, and why exclusion IS the abstention (#3548).** Neither namespace above is enumerated by
`dead-lanes`, so neither produces a row or a verdict of any kind — not `DEAD-*`, not `UNKNOWN-*`,
nothing. AC4 is a rule for a **future** change, not a description of today's output: were a later
change ever to read a **non-refreshing** carrier, a stale pid there must never yield a `DEAD-*`
verdict — it must abstain. The **refreshing** carrier `refs/lane-claims/*` needs no such rule: a
supervisor restamps it every iteration, so an absent or recycled pid there really does mean the lane
is gone and `DEAD-NO-PROCESS`/`DEAD-PID-REUSED` are correct. Do not restate AC4 by naming a verdict
for the unenumerated carriers, and do not restate it in its unqualified form ("a stale pid must never
yield `DEAD-*`") — both are false about the code.

**What lane liveness actually rests on here.** Two things, and neither is a script you can run:

1. **The coordination lead's sweep** — a human-driven loop over the board and the open PRs. It is what
   has been catching stalls in practice, including a lane parked 56 minutes on a missed request.
2. **Two board signatures that mean different things — and NEITHER signature is a verdict.** Both are
   prompts to look (the distinction the recovery row above spells out).
   (a) A HELD `refs/claims/issue-<N>` WITH NO LIVE SESSION IS NOT A VERDICT EITHER: /drive-issue's
   park-and-resume protocol produces exactly that shape for HEALTHY work — a lane blocked on a lead or
   owner answer keeps its claim, arms a `drive-issue-<N>` cron, refreshes its heartbeat and ends its turn.
   It is a dead-lane CANDIDATE only if, in addition, there is no active `drive-issue-<N>` cron, no waiting
   marker (`.drive-issue-state.md`, or an open `coord:*` request on the issue), and the heartbeat and
   branch activity are stale. Without those checks a parked lane and a dead one are indistinguishable, and
   adopting the claim takes it out from under a live lane that is waiting for an answer.
   (b) `Ready` + pushed branch + NO claim ref is AMBIGUOUS and is deliberately NOT classified here: the
   same shape fits parked-by-design work, the #3436 unclaimed-work case, and a lane that died before
   claiming. Nothing distinguishes them mechanically today, so check the branch's last commit time and
   whether a session is driving it; it is not by itself evidence of a lane death, and the rule is to
   treat the signature as a prompt to look, never as a verdict.

**Both are OPERATING MECHANISMS, tracked by #3436 (open, `status:in-review`) — NOT committed tooling.**
There is no `board-signature` script, no sweep command, and no flag in this repository; a `grep` for
either term finds nothing. Do not go looking for one, and do not add its name to this page until
something in `scripts/` actually implements it.

**Where the two readings of signature (b) collide.** The *Reading the board* section above reads
*Ready + branch already on origin* as **parked-by-design**, while #3436 reads the same shape as **work
being performed without a claim**. That conflict is real and unresolved: nothing mechanical
distinguishes them, so an operator has to check the branch's last commit time and whether a session is
driving it. Until #3436 lands a mechanism, treat that signature as a prompt to look, never as a verdict
— and note that neither reading makes it a lane-death verdict. Signature (a), stated in full in item 2
above, is where a suspected death is investigated.

**Known non-lane artifacts a naive lane-liveness scan calls dead (#3548).** Measured, from the ad-hoc
`lane-watchdog.sh` — **not** from `dead-lanes`, which enumerates only claim refs and therefore cannot
report any of these:

- **`lane-3451-mainred`** and **`lane-3401-telemetry`** were both reported `DEAD-NO-SESSION`.
  **Neither is a lane.**
- `lane-3401-telemetry` is `flow-finalize`'s own `telemetry-<N>` worktree, which **every delivery
  creates** — so this is a recurring, structural false positive, not a one-off.
- Acting on either report would have spawned `/drive-issue` **against a nonexistent issue number**.
- Therefore: **an issue number parsed out of a directory name is not evidence that the issue exists.**
  Before acting on any lane-liveness report, confirm the issue number is real AND that the directory
  is a lane rather than a `telemetry-<N>` finalize worktree.

The primitives that DO exist and are worth running by hand on a suspect box: `dmesg | grep -i "out of
memory"` (step 1 below — an OOM kill is the most common cause), `bash scripts/flow/claim.sh status <N>`
(who holds the per-issue lock), `bash scripts/flow/claim-heartbeat.sh list` (machine-level heartbeat
ages — per MACHINE, not per lane), and a filtered board read (`gh project item-list 1 --owner pmcfadin
--query 'status:"In Progress"' …`). `dead-lanes` itself stays correct and useful on a **supervisor**
fleet.

### Diagnostic order when a box stops answering (#3393)

Recorded because getting this order wrong cost a healthy machine. On 2026-08-27/28 the kernel
issued **10 global OOM kills** across two `c7i.4xlarge` workers, every victim a `python3` holding
**20–28 GB** on a 30 GB box. Under memory exhaustion `sshd` cannot fork a session, so the box
presents as *"TCP connects, no banner"* — which reads exactly like a broken instance. It was read
that way: a **healthy box was terminated**, losing a measurement lane's 43 minutes and one
unpushed commit. The signal that would have prevented it was one command nobody ran.

So, in this order:

1. **`dmesg | grep -i "out of memory"`** (or `journalctl -k | grep -i oom`). An OOM kill names the
   victim, its RSS and its cgroup — `task_memcg=…/tmux-spawn-<uuid>.scope` identifies a **lane**
   rather than a system service. This is step 1 because it is cheap, non-destructive, and
   disambiguates the most likely cause.
2. **`bash scripts/flow/claim-heartbeat.sh dead-lanes`** — which lanes lost their process, **on a
   SUPERVISOR fleet**. Run it **on the box in question**: a PID is only checkable where it runs. A row
   annotated `open-pr=yes` is an **orphaned endgame** (#2499): adopt it, never reap it. Since #3393's
   per-lane claim refs every lane on a multi-lane box is reported independently, so this no longer
   covers just one of them. On a supervisor-less `/drive-issue` box, read
   *Lane liveness on a supervisor-less `/drive-issue` fleet* — which PRECEDES this procedure, named by
   heading so a reordering cannot invert the pointer — for what this command's subject set is here;
   any rows it does produce (leftover or manually `stamp`ed refs) are worth inspecting, and the manual
   board reconciliation in that section applies either way.
3. **`df -h`** — a full disk is the other resource-exhaustion story that surfaces as a confusing
   failure (#3379), and it is equally cheap to rule out.
4. **Only then** treat the instance as broken. Note that a soft `reboot-instances` may be
   **silently ignored** on a memory-exhausted box (observed: console stayed at the original boot,
   CPU never dipped); a hard stop/start was required.

Two standing lessons from the same incident. **Memory exhaustion is invisible to any monitor that
iterates existing sessions** — a dead tmux session cannot report itself, so nothing noticed three
silent lane deaths (`lane-1705` twice, `lane-1697` once), each leaving a clean worktree, a held
claim and an open PR; covering that is what `dead-lanes` exists for. And **lane density is the
dial**: 4 lanes per box produced OOM kills and wedges on *both* boxes, while the 1-lane rig box
recorded **zero** and never wedged.

---

## Claude credential reachability (#3733)

**Symptom.** A newly created tmux session on a fleet box cannot start `claude`: it lands on the
first-run login chooser, so a retired lane cannot be replaced. Nothing is wrong with the disk, and
the *same box* was working an hour earlier.

### The mechanism, in six measured facts

1. The ONLY working credential is the environment variable `CLAUDE_CODE_OAUTH_TOKEN`.
   `$CLAUDE_CONFIG_DIR/.credentials.json` holds **empty** `accessToken`/`refreshToken` and
   `expiresAt: 0` — it authenticates nothing (probe it with no token: rc 1,
   `Failed to authenticate: OAuth session expired and could not be refreshed`).
2. The token authenticates **independently of `CLAUDE_CONFIG_DIR`** — token plus a fresh empty
   config dir authenticates fine. The config dir is onboarding/session state, not auth. (An absent
   `CLAUDE_CONFIG_DIR` is what produces the *un-onboarded* first-run picker, which looks like the
   same failure and is not.)
3. The token is provisioned in **`/etc/environment` only** (mode 644), which is read by **pam_env**
   — so it reaches login/ssh sessions and nothing else. `/etc/profile.d/30-agent-ami-data.sh`
   carries `CLAUDE_CONFIG_DIR` but **not** the token.
4. A tmux pane's environment comes from the **tmux server**, fixed at server start. A server that
   predates provisioning yields panes with **neither** variable. *This is the actual field failure.*
5. `tmux new-session <command>` does **not** run the command through a login shell (measured:
   `login_shell=no`), so `/etc/profile.d/*` never executes for a spawned lane either.
6. Therefore **nothing on disk distinguishes a working box from a broken one** — the distinguishing
   state is a long-running process's start environment. That is why the failure is silent until
   dispatch, and why a file check can never find it.

### What bootstrap reports — and why it is a REPORT, not a check

**Nothing here certifies that this box can start a lane, and that is the #3733 lead ruling rather
than an omission.** Section 5c used to print two CERTIFYING verdicts whose passing state was
`VERIFIED` and which `--strict` read. Three consecutive independent reviews each found a NEW
High-severity defect and all three were one shape — **the probe cannot observe the property its
verdict named**: the cold-start probe re-supplies the `/etc/environment` values into its own
throwaway server (tmux propagation, not pam_env delivery); the `claude -p` probe never neutralises
`ANTHROPIC_API_KEY`/`ANTHROPIC_AUTH_TOKEN`/`CLAUDE_CODE_USE_BEDROCK`/`CLAUDE_CODE_USE_VERTEX` (so a
sentinel means *some* credential worked); `[ -d <config dir> ]` runs as the caller, root under the
documented `sudo` invocation (so it says the directory exists *to us*). Each fix was correct and the
family kept regenerating, so the design changed instead. The **four** things the observations cannot
see are documented **in the script** as `LIMITATION 1..5 (#3733)` at their own code sites — five
numbered slots, of which slot 4 is a **record of one that was reclassified as a defect and fixed**
(root wrote into a directory it had already handed to the invoking user, which on a one-user fleet is
a peer lane's symlink opportunity; a hazard that exists whatever the output says is not a limitation
of a report). The slot is kept rather than renumbered so older references still resolve.

Three consequences to work with:

* **No state is named `VERIFIED`.** That is the word a pasted log reads as a certification whatever
  the prose beside it says, so the states now say what was *seen*. The live and cold "both present"
  states are deliberately different tokens — one name for both hid which observation was made.
* **Every run prints a scope note**, `claude-auth-report: OBSERVATIONS-ONLY`, in bootstrap's output
  as well as the CLI's, because bootstrap's output is what an operator pastes.
* **Nothing downstream may act on it.** Both lines go through `info` — never `[ok]`, which is what
  `--strict` reads, and never `[warn]`, which is what makes it fail — so `--strict` neither passes
  nor fails on them. In the CLI **the exit status carries no verdict**: a printed report exits 0
  whatever it found, so the best- and worst-looking boxes are indistinguishable by status and
  `if script --auth; then …` cannot be written. A state rename alone would have left that gate
  intact. `--skip-claude-auth` is loud and is **not** a `[warn]`, unlike its `git-push:`/`gate-pin:`
  siblings: those decline a real verdict, and here there is no green to buy.

**`FAILED` is an accusation about a credential, so it is earned, not defaulted to**: it is emitted
only for a positively identified authentication rejection, and every other unsuccessful probe —
rate limit, outage, quota, crash, bound fired, no sentinel — is `UNMEASURED` with its cause named.
The matchers are ordered killed-by-bound -> transport -> service failure -> **rejection last**, so a
response naming both a benign cause and an authentication wording takes the non-accusing answer:
ambiguity is not evidence, and telling someone to replace a working token because the API was
rate-limiting is exactly the "measurement of something adjacent, reported as the thing itself"
failure this section exists for. `NO-SERVER` is UNMEASURED-class — since the cold-start probe it
means "the isolated probe could not run", **not** merely "no server was running": a serverless box
is measured, not excused (below).

```
claude-auth:      PROBE-ANSWERED | NOT-PERSISTED | FAILED | UNMEASURED
claude-tmux-env:  live server: SERVER-CARRIES-BOTH | SERVER-STALE | SERVER-MISSING
                               | SERVER-INCOMPLETE | SERVER-CONFIG-STALE | SERVER-CONFIG-NODIR
                  no server:   COLD-START-DELIVERS-BOTH | COLD-START-MISSING
                               | COLD-START-INCOMPLETE | COLD-START-NODIR
                  either:      NO-SERVER | UNMEASURED
claude-auth-report: OBSERVATIONS-ONLY   <- printed every run; neither line above certifies the box
```

They are two lines because they observe different things and the operator actions differ:

| Observation | What it means | What to do |
|---|---|---|
| `claude-auth: PROBE-ANSWERED` | A bounded `claude -p` run whose environment **carried** the persisted value returned rc 0 **and** the sentinel. It does **not** say the persisted value is what authenticated — the line names any alternate credential that was also present (LIMITATION 2). | Nothing. This is not a green light; it is one observation. |
| `claude-tmux-env: SERVER-CARRIES-BOTH` | The running server's **global** environment carries a matching token and a matching `CLAUDE_CONFIG_DIR` that exists as seen by this process. No pane was spawned (LIMITATION 5) and the directory's usability to the agent is unobserved (LIMITATION 3). | Nothing. |
| `claude-tmux-env: COLD-START-DELIVERS-BOTH` | No live server. A throwaway one, **started with the two values this process read out of `/etc/environment`**, passed both to a pane. So it observes tmux **propagation**, not pam_env **delivery** — a line pam would have dropped is invisible to it (LIMITATION 1). | Nothing. |
| `claude-auth: NOT-PERSISTED` | No `CLAUDE_CODE_OAUTH_TOKEN` line in `/etc/environment`. | Provision it (below). Bootstrap deliberately never writes the credential itself. |
| `claude-auth: FAILED` | A token IS persisted and the API **positively identified it as rejected** (an authentication error, a 401, `Failed to authenticate`, `Please run /login`). | Replace the **value**; bootstrap never rewrites an existing one. |
| `claude-auth: UNMEASURED` | The probe did not succeed and **did not identify a credential rejection**: a rate limit, an API outage or overload, an exhausted quota, an unreachable network, a CLI crash, the hard bound firing, no `claude` on PATH, no `timeout` able to enforce a hard bound, an unreadable `/etc/environment`, or rc 0 with no sentinel. | Read the named cause and resolve it, then re-run. **Do NOT replace the token on this evidence**: `FAILED` is the only state that means the credential was rejected. |
| `claude-tmux-env: SERVER-MISSING` | A tmux server is running and carries no token. **THE field failure.** | `--fix-claude-auth`, **explicitly** — it overwrites; see the note below. |
| `claude-tmux-env: SERVER-STALE` | The server's token **differs** from the persisted one. Worse than missing: everything looks provisioned. | `--fix-claude-auth`, explicitly. **Read the note below first**: this is the state where an overwrite can destroy the box's only working credential. |
| `claude-tmux-env: SERVER-INCOMPLETE` | Token matches, `CLAUDE_CONFIG_DIR` absent — the un-onboarded picker (fact 5). | `--fix-claude-auth`, explicitly. |
| `claude-tmux-env: SERVER-CONFIG-STALE` | Token matches, but the server's `CLAUDE_CONFIG_DIR` **differs** from the persisted one — panes are pointed at a directory nobody provisioned. | `--fix-claude-auth`, explicitly. |
| `claude-tmux-env: SERVER-CONFIG-NODIR` | The config dir matches the persisted value and **that directory does not exist** — as seen by this process (LIMITATION 3). Seeding writes the same missing path back, so it cannot help. | Create the directory, or correct the `CLAUDE_CONFIG_DIR` line in `/etc/environment`, then `--fix-claude-auth`. |
| `claude-tmux-env: COLD-START-MISSING` | No server is running, and a throwaway one started from the persisted environment handed its pane **no token**. The next real server will not either. | Provision the token (below). |
| `claude-tmux-env: COLD-START-INCOMPLETE` | A new server would deliver the token but **no `CLAUDE_CONFIG_DIR`** — `/etc/profile.d` never reaches a spawned pane (fact 5). | Add a `CLAUDE_CONFIG_DIR=` line to `/etc/environment`. |
| `claude-tmux-env: COLD-START-NODIR` | A new server would deliver both, but that config directory does not exist. | Create it, or correct the `/etc/environment` line. |
| `claude-tmux-env: NO-SERVER` | No server is running **and** the isolated cold-start probe could not run (no `timeout`/`gtimeout` able to enforce a **hard** bound — one that escalates to SIGKILL via `--kill-after=` or `-k` — no private working directory, no `sha256sum`/`shasum` to compare the delivered credential BY VALUE, the directory could not be handed to the invoking agent, tmux would not start), **or** the pane received a token that is not the persisted one. **UNMEASURED-class.** | Resolve the named cause and re-run. |
| `claude-tmux-env: UNMEASURED` | Nothing could be read: no `tmux`, no enforceable hard bound for the read, the server did not answer within its bound, **or the tmux server to inspect could not be identified** (see the sudo note below). **Never a fall back to whichever UID the process happens to be.** | Resolve the named cause and re-run. |

**WHOSE tmux SERVER? THE INVOKING AGENT'S.** A tmux client with no `-S`/`-L` talks to the
**current UID's** default server, and bootstrap both documents and prints
`sudo bash scripts/bootstrap-agent-machine.sh --yes`. Under sudo, therefore, an unqualified
`tmux show-environment -g` inspects **root's** server while the agent's own — the one that
actually spawns lanes — stays broken; root usually has no server at all, so the read fell
through to the cold-start probe, which measures the persisted FILE and reports a delivery. Under
the old design that was a false `VERIFIED` on a box that still cannot start a lane — one of the
findings that led to the demotion above. Section 5c resolves the invoking
identity from `SUDO_USER` (cross-checked against `SUDO_UID`) and runs **every** tmux
operation — the read, the repair, and the throwaway cold-start server — as that login via
`runuser`/`sudo -n`. Where the identity cannot be resolved (an unresolvable login, a
self-contradicting sudo record, no delegation tool) the verdict is `UNMEASURED` and
`--fix-claude-auth` **refuses**: falling back to the current UID is the permissive branch
wearing a default's clothes. Every tmux call is also **hard-bounded**, so a server that accepts
a connection and never answers reports `UNMEASURED` instead of hanging an unattended
provisioning run. One predicate is **not** delegated and is declared rather than fixed:
`[ -d <config dir> ]` runs as the caller (LIMITATION 3), so on the `sudo` path it answers about
root's access and not the agent's.

**A box with no tmux server is measured, not excused.** That is the normal state of a freshly
provisioned machine at the moment `.agent-ami/profile.yaml` runs bootstrap with `--strict`, so a
blanket non-pass there would red this check on its primary use case with no way out
(`--fix-claude-auth` deliberately excludes the serverless states — there is no server to seed).
Instead the
answerable question is asked: *would a newly created server deliver the credential to a pane?* A
throwaway tmux server is started **on a private socket inside a private working directory**, from
an environment **reconstructed from `/etc/environment`** with the inherited credential scrubbed;
one pane reports what it received; the server is killed in a trap on every exit path including
signals, and the socket goes with the directory. The pane is spawned exactly the way a lane
spawner spawns one — `tmux new-session <command>` runs the command through `sh -c`, **not** a
login shell — or the probe would measure the wrong thing. What is reconstructed is the
**credential** environment, not a whole PAM session: `PATH`/`HOME` and the rest are the running
process's, because they are what make the probe runnable and are not the subject.
**And note what that reconstruction costs in claim strength (LIMITATION 1):** the probe SUPPLIES
those two values, so what it observes is that a tmux server propagates its start environment to a
pane — not that pam_env would have delivered them in the first place. A `/etc/environment` line pam
silently drops is invisible to it. That is deliberate: measuring pam_env means creating a PAM
session, which needs privilege this may not have and would be a login on the operator's box.

Two measured details worth keeping. A box that has **never** started a server does not say
`no server running` — tmux 3.4 says `error connecting to <socket> (No such file or directory)`,
and only a **stale** socket (server died, file remains) gives the familiar wording; both are
recognised, and anything else (a permission denial, `lost server`) stays `UNMEASURED`. And a tmux
**client** started inside a pane connects to the server named in `$TMUX` and **ignores**
`TMUX_TMPDIR` — which is why the probe scrubs `TMUX`/`TMUX_PANE`.

Run the same two checks by hand at any time, without the rest of bootstrap:

```bash
bash scripts/claude-auth-capability.sh --report          # both lines
bash scripts/claude-auth-capability.sh --auth            # the credential probe alone
bash scripts/claude-auth-capability.sh --tmux-env        # the pane-reachability observation alone
bash scripts/claude-auth-capability.sh --fix-tmux-env    # seed the running server, then re-report
```

**Read the lines, not the status.** `--auth`/`--tmux-env`/`--report` exit 0 whenever a report was
printed; only a usage error and a refusal to produce a report at all are non-zero. `--fix-tmux-env`
is the exception, because seeding is an *action* that can fail — and it seeds **unconditionally**,
which is what makes it the deliberate override.

The `*-CARRIES-BOTH`/`*-DELIVERS-BOTH` states are an **affirmative match**, not an absence of bad
news: the `CLAUDE_CONFIG_DIR` must **equal** the persisted value **and** that directory must
**exist**. Testing only "is it absent" is the two-valued predicate that always picks the
permissive answer, and a wrong config dir produces exactly the reported symptom.
**Two declared residuals:** *exists* is not *onboarded* — whether the directory holds usable
onboarding state is deliberately not probed, because that means depending on an internal
JSON field shape that can change upstream — and *exists* is *exists to this process*
(LIMITATION 3).

The probe reads the token from `/etc/environment`, **scrubs the inherited one** (and `BASH_ENV`/
`ENV` with it), and requires **both** rc 0 **and** a sentinel back from a bounded `claude -p`. The
token value is never printed by any of these — they report `SET`/`ABSENT`/`MATCH`/`DIFFERS`.
**It scrubs ONE credential and leaves the others (LIMITATION 2):** `ANTHROPIC_API_KEY`,
`ANTHROPIC_AUTH_TOKEN`, `CLAUDE_CODE_USE_BEDROCK` and `CLAUDE_CODE_USE_VERTEX` are inherited
untouched and `claude` authenticates from any of them, so a returned sentinel means *some*
credential in that environment worked. They are deliberately **not** scrubbed — silently changing
what the probe authenticates with would be a behaviour change hiding behind a report — and the
`PROBE-ANSWERED` line **names** the ones it found instead.

**Which half of the scrub is the mechanism** — measured by deleting each flag and re-running
`scripts/tests/test_claude_auth_capability.sh`, because a scrub nothing can falsify is a scrub
nothing asserts. `-u BASH_ENV` on the `--auth` probe *is* the mechanism: a non-interactive bash
sources `$BASH_ENV` **after** `env KEY=<persisted>` has run, so a file re-exporting the credential
overrides the value the probe was deliberately handed and yields an answered probe about the
inherited one. `-u CLAUDE_CODE_OAUTH_TOKEN` is the mechanism in the **cold-start** probe, where the
re-supplying assignment is conditional. In the `--auth` probe that same flag is **belt**, redundant
by construction (the assignment always follows and always wins) — kept, and *declared*, rather than
covered by a case asserting something already true. `-u ENV` is belt everywhere: `$ENV` is read only
by an interactive POSIX shell.

**The sentinel is a transformation, not an echo.** The prompt asks for the UPPERCASE form of a
lowercase word and never contains `CQLITE_CLAUDE_AUTH_OK` itself, so `grep -qF "$SENTINEL"` cannot
be satisfied by anything that merely repeats its own argv — which the repo's own test stub did.

**The `/etc/environment` grammar is measured, not assumed.** `/etc/pam.d/sudo` carries
`pam_env.so readenv=1`, so appending a probe line and reading `sudo env` shows exactly what pam_env
delivers. It skips leading whitespace, then drops an **exact 7-byte `export `** prefix (`export K=v`
and `  export K=v` are delivered; `export  K=v`, `export<TAB>K=v`, `exportK=v` and `setenv K=v` are
not), and the key runs to the first `=` with **no** whitespace before it. The parser matches that
exactly — an over-permissive anchor would report `PERSISTED` for a line no session receives.
Three further refusals, each the non-permissive answer to an unknown: a **symlink** at the env-file
path is `unreadable` (what it points at is not the file pam_env consumes) **including a dangling
one**; a `grep` that **errors** (rc >= 2) is `unreadable`, never the affirmative "no token line
here"; and an unparseable line is an absence of evidence, never a mismatch.

### Recovery: a box whose new sessions hit the login chooser

```bash
# 1. WHICH HALF is broken. Do not guess: the two halves have different remedies.
bash scripts/claude-auth-capability.sh --report

# 2. If `claude-auth:` is NOT-PERSISTED — provision the credential. Its OWN LINE, no inline
#    comment (pam_env takes a trailing `# ...` as part of the value), root:root 0644. A
#    leading `export ` (exactly one space) is fine — pam_env strips it — but `setenv `,
#    `export  ` with two spaces and `K = v` are all silently NOT delivered.
#    `$TOKEN` MUST be expanded by YOUR shell, not root's: `sudo sh -c '... "$TOKEN" ...'`
#    hands the single-quoted text to a ROOT shell that never received the variable, so it
#    appends `CLAUDE_CODE_OAUTH_TOKEN=` — an EMPTY value, which the check then correctly
#    reports as NOT-PERSISTED while the line looks provisioned. Pipe instead:
printf 'CLAUDE_CODE_OAUTH_TOKEN=%s\n' "$TOKEN" | sudo tee -a /etc/environment >/dev/null

# 3. Repair the RUNNING tmux server. This is the step that fixes the field failure, and it
#    writes NOTHING to disk. It is UNCONDITIONAL and OVERWRITES -- read the note below.
#    `--yes` does NOT do it: seeding is your decision, not an unattended run's.
bash scripts/bootstrap-agent-machine.sh --fix-claude-auth
#    ...or by hand:
tmux setenv -g CLAUDE_CODE_OAUTH_TOKEN "$TOKEN"
tmux setenv -g CLAUDE_CONFIG_DIR "$CLAUDE_CONFIG_DIR"

# 4. Confirm by READING THE LINES, never by "the command exited 0" -- these entry points
#    exit 0 whenever a report was printed, and no state on either line certifies the box.
bash scripts/claude-auth-capability.sh --report
```

A pane created **before** the seeding keeps its old environment — tmux copies the server
environment at pane creation. Kill and respawn the lane; you do not need to restart the server.

**Seeding is YOUR decision. `--yes` never does it, and `--fix-claude-auth` does it
unconditionally.** The hazard is real and unchanged: on a box whose persisted token is bad while
the *running server* holds a working one, seeding overwrites the working value with the broken one
and every lane spawned afterwards fails to authenticate — the repair breaking a working box,
unattended, since `.agent-ami/profile.yaml` runs bootstrap this way. `SERVER-STALE` is exactly
where it fires, because "the server's token differs from the persisted one" reads the same whether
the server's copy is the stale one or the only good one on the machine, and nothing in that line
can tell those apart.

Round 3 met that with a precondition — seed only when `claude-auth:` is `VERIFIED` — and **that
precondition is withdrawn (#3733 lead ruling)**, because `VERIFIED` was a *proxy* that could be true
on a box whose persisted credential was never what authenticated (LIMITATION 2): false-positive in
precisely the direction that causes the harm, which makes it **worse than no gate**, since it
licenses the unattended seeding it cannot justify. Removing the gate while keeping the seeding
under `--yes` would be that harm with the excuse deleted, so both went.

So: **`--yes` reports and names the command**; an explicit `--fix-claude-auth` seeds and **states
at the point of action** that it overwrites a value nothing here has validated. The hand-run
`bash scripts/claude-auth-capability.sh --fix-tmux-env` is the same operation without the rest of
bootstrap. Before you run either on a `SERVER-STALE` box, decide which copy is the good one — that
is the judgement no mechanism in this repo can make for you, and pretending otherwise is what
three review rounds kept finding.

**A note on what #3733 delivered, so nobody re-reads this section as a green light.** Its AC1 —
the six-fact diagnosis above — stands and is the durable value: it is what tells you where to look
when a lane will not start. **AC3, a verified cold-start capability check, was NARROWED by owner
ruling**: the observations remain, no certification is claimed, and no replacement mechanism is
proposed. The four things they cannot see are `LIMITATION 1..5 (#3733)` — slot 4 being a fixed one, kept as a record — in
`scripts/claude-auth-capability.sh`, marked at their own code sites.

### Can an unattended box be re-authenticated without a human at a browser? **Yes.**

This is #3733's AC4, answered explicitly. The credential is a **static, shareable gateway token**
(owner ruling on the issue), so provisioning a box is a **file copy plus seeding the tmux server** —
steps 2 and 3 above. **There is no interactive OAuth step**, no browser, and no per-machine login.

One caveat, stated plainly because it is the whole failure mode:

* A tmux server started **before** provisioning keeps a stale environment until it is seeded
  (`tmux setenv -g`) or restarted. Provisioning the file alone does **not** reach it.
* `tmux new-session <command>` bypasses **both** pam_env and `/etc/profile.d`, so a lane-spawning
  script must pass `-e CLAUDE_CODE_OAUTH_TOKEN=… -e CLAUDE_CONFIG_DIR=…` explicitly, or rely on a
  server that has already been seeded.

### Why this is a bootstrap check and not a monitor

Fact 6: no file check can see it. The distinguishing state lives in a running process, so the
question has to be asked of that process — which is exactly what `tmux show-environment -g` does,
and exactly what nothing did before #3733. The probe half deliberately makes one real, bounded,
billed `claude -p` call; `--skip-claude-auth` (or `CQLITE_BOOTSTRAP_SKIP_CLAUDE_AUTH=1`) declines
it **loudly** — it emits `claude-auth: OPT-OUT`. That line is **not** a `[warn]` and does not
withhold "All checks green.", unlike its `git-push:`/`gate-pin:` siblings: those decline a real
verdict, so an opt-out that bought a green would be a vacuous certification, whereas section 5c
certifies nothing and there is no green to buy. Making it a `[warn]` would make `--strict` fail on
a line that is not a verdict.

**And the section is still not a monitor, for a second reason now.** It observes, so a green run
proves nothing about the next dispatch. The thing that tells you a lane is actually broken is a
lane failing to start — see "Diagnostic order for a box that stops answering".

**Bootstrap writes the token to no file.** `/etc/environment` already holds it; a second 644 copy
would buy nothing, and it is refused on the precedent of
`openspec/specs/worker-environment-preflight/spec.md` — whose "SHALL NOT write the token
itself to disk" clause is stated for `$GH_TOKEN` under the git-credential requirement, so it
is the precedent for this rule rather than a clause that already names this credential.
`tmux setenv -g` does pass the value in `argv` (briefly visible in `ps`) — declared rather than
worked around, because tmux offers no stdin form and the same value is already world-readable in
`/etc/environment` on these boxes.

---

## The two dials you own

1. **Approval cadence** (Seam 1). The system's real rate limiter — median 29.4h backlog vs
   16-minute merges. Batch approvals at session start; keep the Ready column non-empty.
2. **Fleet size.** Each additional machine = `git clone` + bootstrap + `/worker` = one more
   concurrent issue. Coordination cost of machine N+1: one claim-ref push per claim.

*Written 2026-07-06 from the agentic-workflow audit. Update this page in the same change whenever
flow-* doctrine changes (doctrine-current rule).*

## perf seam containment — why (relocated from `scripts/perf-capability.sh`)

These rationales were moved out of the script's comments so the reviewed diff stays under
roborev's inline limit (#3257) — the information is preserved verbatim, and each code site keeps
a short pointer here. Boundary statements, residual pointers (#3323) and retraction records were
deliberately NOT moved: those must be met at the code site.

### line-safety-serialization

```
perf_capability_path_lines_ok: rc 0 iff <path> contains NO CR and NO LF.

WHY A SEPARATE PROPERTY FROM CONTAINMENT (issue #3261, roborev round 3). This one is not a
containment defect at all — the path IS contained — it is a SERIALIZATION defect, which is why
nine rounds of containment work never touched it. `perf_capability_sysctl_search_path` emits its
answer ONE ENTRY PER LINE and `perf_capability_competing_files` reads it line-wise, so a directory
legitimately inside the sandbox but NAMED with an embedded newline —
`<root>/name<LF>/etc/sysctl.d` — passes every containment check and is then SPLIT into two
entries, the second of which is the host's real `/etc/sysctl.d`. One contained path became two
paths, one of them production.
The repo already knows this class: CLAUDE.md records the roborev guard's own `-z` invariant for
exactly this reason — a newline-delimited path set is not a safe representation of paths, and the
fix there was to stop using one. Here the answer is the cheaper half of the same lesson: REJECT
the characters at the boundary rather than escape them downstream or re-plumb every consumer to
NUL. A path with a newline in it has no legitimate use as a perf seam, so refusing it costs
nothing and removes the ambiguity at its source. CR is rejected with LF because a CRLF host file
would otherwise leave a stray `\r` inside a resolved entry.
```

### priv-tool-destination

```
perf_capability_priv_tool_ok <resolved-path> <tool-name>: rc 0 iff this privileged executable's
RESOLVED destination is positively contained beneath the PROVEN sandbox root. Refuses loudly.

WHY (issue #3261 AC4 — the EIGHTH escape from this guard family, and the first about an
executable rather than a path). The shim dir was trusted TEXTUALLY: `/usr` is absolute and
genuinely CONTAINS `/usr/bin/sudo`, so "inside the declared shim dir" passed; and a SYMLINK to
the real `sudo`/`sysctl` placed inside a genuine shim dir is spelled locally while resolving to
the host's binary. Either one let a privileged test-mode bootstrap run a real
`sysctl --system` and reconfigure the host kernel — the exact mutation the marker promises
cannot happen. Same discipline as the paths: the declared NAME is not the DESTINATION, so the
destination is what is judged, positively, against a root that had to prove itself. The FILE
form does the work (canonical parent + basename, and a symlinked final component is refused),
which is also why a `/usr` shim dir fails: /usr/bin is not inside the sandbox.
```

### dropin-path-write-target

```
perf_capability_dropin_path: the path a root `tee` is pointed at. The DIRECTORY's gate lives in
perf_capability_sysctl_dir (the single source of that directory) and RESOLVES the destination,
so there is deliberately no second copy of it here — one gate, not a prohibition a future entry
point could skip (R6-2).

BUT DIRECTORY CONTAINMENT IS NOT WRITE-TARGET CONTAINMENT (issue #3261 AC1). A contained
directory says nothing about where its ENTRIES point, and `tee <path>` opens
O_WRONLY|O_CREAT|O_TRUNC and FOLLOWS a symlink — so a symlink at the managed basename inside a
perfectly-contained directory aimed the privileged write at the LINK'S TARGET, anywhere on the
box. The write TARGET is therefore validated too, as an O_NOFOLLOW-equivalent refusal: a
symlink at the managed name is rc 1 + empty + a named reason, for every consumer at once
(this function is the single source of the path). The complementary half is
perf_capability_dropin_install, which REPLACES the directory entry instead of writing through
it, closing the window between this check and the write.
```

### tool-compatibility-is-exercised-here-in-the-priv

```
# TOOL COMPATIBILITY IS EXERCISED HERE, IN THE PRIVILEGED SHELL (roborev round 17, Medium — a
# defect in the round-16 fix). The previous probe ran in the CALLERS PATH, but this shell is
# entered through sudo, which applies its own secure_path: the two can resolve DIFFERENT stat and
# mv binaries, so a caller-side check could pass while the privileged tools are incompatible, or
# refuse while they would have worked. Same lesson already applied to mktemp here — the block
# holding privilege re-checks rather than trusting what someone else established.
# And the flags are EXERCISED, not grepped: reading mv --help proves a help string mentions
# --no-target-directory, not that rename-over-a-name behaves. Two throwaway entries in the
# already-validated directory are renamed one over the other, which is exactly the operation the
# install depends on. rc 2 = UNSUPPORTED HOST, distinct from rc 1 = REFUSED.
# Probed on `/`, a KNOWN-VALID operand, not on $d (roborev round 23, Medium): statting the
# DESTINATION here conflated "no GNU stat" with "destination missing", so an absent /etc/sysctl.d
# returned rc 2 and bootstrap suppressed a remedy that would have helped. Destination problems are
# rc 1, reported by the owner/mode read further down; rc 2 means the TOOL is incompatible.
```

### usr-lib-sysctl-d-50-x-conf-outright-and-reporti

```
/usr/lib/sysctl.d/50-x.conf outright, and reporting the masked one would name
a file that is not in effect.
ORDERING the survivors are applied in lexicographic BASENAME order regardless of which
directory they came from; the LAST assignment wins. /etc/sysctl.conf is applied
AFTER every drop-in, so it wins on grounds unrelated to its name and gets its
own verdict rather than a sort comparison.
WHY THE WHOLE PATH (review R5-4). Scanning only /etc/sysctl.d meant a later-sorting file in
/run/sysctl.d or /usr/lib/sysctl.d could override our drop-in while bootstrap reported NO
competitor — recreating the "it silently reverts and nobody knows why" mystery this
diagnostic exists to end.

```

### every-globbed-file-is-validated-in-test-mode-not

```
# EVERY GLOBBED FILE IS VALIDATED IN TEST MODE, NOT JUST ITS DIRECTORY (roborev round 11,
# Medium). The scan validated the containing DIRECTORY and then trusted whatever the glob
# produced inside it — but `[ -f ]` and `grep` both FOLLOW symlinks, so a link sitting inside a
# perfectly contained sandbox directory and pointing at a real host `*.conf` was read, and its
# contents fabricated "a competitor sets these keys" diagnostics out of HOST state. That is a
# hermeticity escape in the DIAGNOSTIC path: the numbers a test asserts on would come from the
# box rather than the fixture. Same lesson as the write path, one surface over — a contained
# directory says nothing about where its ENTRIES point.
# `perf_capability_sandbox_file_ok_resolved` is the AC3 predicate, reused rather than
# reimplemented: it refuses a symlink outright and requires the canonical parent-plus-basename
# to be strictly inside the declared sandbox. FAILS THE SCAN CLOSED, because a competitor we
# declined to examine is exactly the UNKNOWN this diagnostic exists to report rather than hide.
# Production is untouched: without CQLITE_PERF_TEST_MODE there is no sandbox and the real
# /etc/sysctl.d files are the legitimate subject.
```

### mechanism-order-setpriv-util-linux-a-plain-setre

```
Mechanism order: `setpriv` (util-linux; a plain setresuid — no PAM, no session, no shell),
then `runuser`, then `sudo -n -u`. Two of the three take the VALIDATED NUMERIC ids and
never a name — `setpriv --reuid/--regid` and `sudo -u '#<uid>'` (sudo's documented
numeric-uid form) — so no name has to be trusted (R4-2); `runuser` accepts only a name and
is used ONLY with a passwd-confirmed one. The `#<uid>` token survives the caller's
word-split intact: `#` starts a comment only while TOKENISING a source line, and this value
arrives by EXPANSION afterwards. The prefix holds only literal tokens plus a validated
numeric uid/gid or a confirmed name, so the caller may word-split it. A non-zero rc is NOT
an error to fail on: it is the caller's cue to label the functional result as what it is —
not evidence about an unprivileged process — and to let the /proc token be the authority.
```

### perf-capability-verify-prefix-word-the-functiona

```
perf_capability_verify [prefix-word...]: the FUNCTIONAL verification (AC2). A bootstrap
that silently leaves a box unprofileable is the failure mode being fixed, so the verdict
comes from RUNNING the collection the doctrine mandates — `perf stat -C 0 -e cycles` — and
requires BOTH exit 0 AND a non-zero cycle count: `perf stat` exits 0 while printing
`<not supported>`/`<not counted>` (and a virtualised PMU can report a flat 0), so an
rc-only check is exactly the false green this exists to prevent.

Any arguments are a command prefix the collection runs under — the privilege-dropping
prefix above. This function makes NO claim about identity: it runs what it is given and
reports the counter. Deciding WHOSE capability was measured is the caller's job, because
the caller owns the verdict.

```

### perf-capability-dropin-current-rc-0-iff-the

```
perf_capability_dropin_current: rc 0 iff the drop-in exists with EXACTLY the managed
bytes (the idempotency test — a matching file means write nothing). The compare is an
in-shell string compare, NOT `diff -q`: on a box without diffutils `diff` exits 127,
which reads as "different" on every run — so bootstrap would re-write the file each time
AND then report it could not write it.

TRAILING NEWLINES ARE PART OF THE BYTES (review R4-4). `$( )` strips EVERY trailing
newline, so comparing two command substitutions made a file missing its final newline —
or carrying extra trailing blank lines — compare EQUAL: "byte-exact" was a false claim
and such a file was never rewritten. The file side is read with `read -r -d ''`, which
consumes it verbatim (builtin, no `cat`/`diff` dependency), and the canonical side carries
an in-substitution sentinel so its own final newline survives the stripping.

A NUL BYTE IS THE THIRD SPELLING OF "NOT EXACT" (review R5-3). `read -d ''` stops at a NUL
and returns SUCCESS with only the bytes BEFORE it, so canonical content + NUL + ARBITRARY
trailing bytes compared EQUAL and was judged current. Read's rc is therefore load-bearing:
rc 0 means a NUL was consumed — not our text drop-in, and the rest never even seen — so it
is NOT current; only rc != 0 (EOF, whole file in `got`) may be compared.
```

### the-sentinel-must-not-swallow-the-generator

```
# THE SENTINEL MUST NOT SWALLOW THE GENERATOR'S STATUS (roborev round 9, Medium). `printf X`
# exists so a trailing newline survives command substitution, but it also RAN LAST, so the
# substitution reported ITS status and a failed content generator looked like success: `want`
# became bare "X", and against an empty file `${got}X` is also "X" — equal, so a broken
# generator reported the drop-in ALREADY CURRENT. A positive verdict from an unmeasured state,
# which is the exact shape this repo has a standing rule against. The rc is now captured
# BEFORE the sentinel and re-raised as the subshell's exit status, so both properties hold.
```

### perf-capability-proc-read-outvar-name-the-cu

```
perf_capability_proc_read <outvar> <name>: the CURRENT kernel value read straight from
/proc/sys/kernel/<name> into <outvar> (rc 1 + <outvar> emptied when unreadable). NEVER
trust a `sysctl -w`/`--system` return code — a write can report success while the value
does not take (container, read-only sysfs, a competing drop-in applied later), and it can
report FAILURE for an unrelated entry while ours applied fine. Read back.
Fully fork-free: `read` is a builtin, the directory comes back through a variable, and
nothing here is wrapped in `$( )` — this sits in the gate's summary path, which may not
grow a process for a diagnostic line. `read` returns non-zero at EOF on a file with no
trailing newline yet still assigns, so emptiness — not read's rc — is the failure test. It
also propagates the test-mode sandbox refusal (R4-3): with no seam inside the sandbox there
is no directory to read, and that is rc 1, never the real /proc.

```

### b951

```

WHY (review R4-2). `runuser -u <name>` / `sudo -u <name>` drop to whatever the NAME
resolves to, not to the numeric ids we validated, and SUDO_USER/SUDO_UID are independent
environment strings: `SUDO_UID=1000 SUDO_USER=root` (stale or hand-set) would run the probe
AS ROOT while the code reported a successful drop — a false VERIFIED again. So a name is
usable only once the passwd database confirms it IS the validated uid/gid. The shape check
is equally load-bearing: the prefix is word-split by the caller, so a name containing
whitespace or glob characters could inject extra argv tokens.
```

### event-name-matching-must-accept-a-qualified

```
# Event-name matching must accept a QUALIFIED cycle event: on a hybrid-PMU CPU (Intel
# 12th-gen+ P/E cores) perf emits one row per PMU (`cpu_core/cycles/`, `cpu_atom/cycles/`),
# commonly with `<not supported>` on the sibling that did not run — so a parser keyed on a
# literal leading `cycles` reports `no-cycles-row` on a perfectly good collection. Normalise
# the event field (drop PMU prefix, trailing `/`, any `:u`/`:k` modifier) and take the FIRST
# row with a positive numeric count; keep the first matching row's raw field as the fallback
# so the `<not supported>` / zero diagnostics below still fire when none is positive.
```

### structurally-the-staging-entry-is-created-b

```
# ...structurally: the staging entry is created by `mktemp` with a random-suffix template, no
# hardcoded staging literal survives, the rename carries `-T`, and the WHOLE staged install is ONE
# privileged invocation (issue #3261 roborev round 2).
#   THE LAST PROPERTY IS HYGIENE, NOT THE FIX, and this comment previously said otherwise. roborev
#   round 3 corrected it: a single `sh -c` sequences ONE PROCESS's commands and is not mutual
#   exclusion against other processes, which run concurrently on other CPUs regardless of how we
#   grouped ours. Consolidation NARROWS the create-to-reopen window; it does not close it. It is
#   still worth pinning — a split back into `mktemp` in one privileged call and the write in another
#   would re-widen the window while every behavioural assert stayed green, which no after-the-fact
#   observation can catch — but it is pinned as hygiene, not as the guarantee.
```

### and-the-precondition-that-actually-closes-t

```
# ...and THE PRECONDITION THAT ACTUALLY CLOSES THE STAGING RACE (issue #3261, roborev round 3): a
# drop-in directory writable by anyone less privileged than the writer is REFUSED before anything is
# staged. Three rounds of this defect were each answered by trying to make the race unwinnable
# (unpredictable name, then one privileged invocation); neither works, because a single `sh -c`
# sequences OUR commands and says nothing about other processes on other CPUs. Removing the
# attacker's precondition does work — with no one able to create or replace entries in the
# directory, there is no actor to race, whatever the timing.
# The negative control is the whole point of the group: a check that refuses everything would pass
# the two refusal cases and be useless, so a correctly-owned 0755 directory must still install.
```

### a-short-mode-from-stat-c-a-must-not-bypass

```
# ...a SHORT MODE from `stat -c %a` must not bypass the write-bit check (roborev round 5, High).
# WHY A SHIM AND NOT A REAL chmod: `%a` only drops below three digits when the OWNER digit is 0,
# and a directory its owner cannot enter fails containment long before the mode check — so the real
# bypass is NOT reachable through an actual chmod under test mode. It IS reachable as root in
# production, where root ignores permission bits and enters a mode-0033 /etc/sysctl.d happily while
# group and other retain write. So the honest reproduction is to feed the parser the short string a
# root `stat` would really print, against an enterable directory. Without the zero-padding this
# reports "33", the suffix-strip leaves the permission field EMPTY, no write-bit pattern matches,
# and a group- AND world-writable directory is ACCEPTED.
```

### 1g-ii-ac2-medium-the-inversion-regressed-sym

```
1g-ii. AC2 (Medium) — the inversion REGRESSED symlink rejection on the READ path. The
fork-free proc check judges the SPELLING, so a symlink INSIDE the sandbox pointing at
the real /proc/sys/kernel satisfies containment: the run then reports a capability
token derived from the HOST's real controls while claiming to have read a stand-in.
That is a FABRICATED verdict, which is worse than a refusal, and it is a regression
against the pre-inversion behaviour. The token path is contractually fork-free, so the
fix must reject symlinked COMPONENTS with builtins only.
```

### 1g-iv-ac4-high-the-guard-authorizes-executab

```
1g-iv. AC4 (High) — the guard authorizes EXECUTABLES, and never resolved them. Two escapes,
one shape: an absolute shim dir that is not in the sandbox at all (`/usr` — it does
contain the real /usr/bin/sudo, so the textual "inside the declared dir" check
PASSED), and a SYMLINK to the real tool sitting inside a genuine shim dir (spelled
locally, resolving to the host's binary). Either one let a privileged test-mode
bootstrap execute a real `sysctl --system` and reconfigure the host kernel.
`$ac4_sys` stands in for `/usr` PORTABLY: an absolute directory OUTSIDE the declared
sandbox root that holds tools named `sudo`/`sysctl`. Asserting against the literal
/usr would make the case depend on whether this host happens to ship sudo there.
```

### 1c-iii-b-the-containment-boundary-and-the-sand

```
1c-iii-b. THE CONTAINMENT BOUNDARY AND THE SANDBOX ROOT ITSELF (issue #3249 review
R6-1/R6-2). Containment is only as good as its boundary and its root:
* `/tmp/sandboxevil` must NOT count as inside `/tmp/sandbox` — a plain string
prefix would accept it, which is why the `/` boundary is explicit;
* a path genuinely inside the declared root must still WORK, so the guard is
not vacuously refusing everything (the failure mode a negative-only test
cannot see);
* the ROOT must PROVE itself — unset, relative, `//`-spelled, non-existent, or
an existing directory with no stamp are all refusals NAMING
CQLITE_PERF_TEST_SANDBOX. Without that, `CQLITE_PERF_TEST_SANDBOX=/etc`
would make containment vacuous, and the inversion would have bought nothing.
```

### 1f-the-gate-is-singular-and-unskippable-a-stru

```
1f. THE GATE IS SINGULAR AND UNSKIPPABLE — a STRUCTURAL audit (issue #3249 review R6-2).
R6-2 was not a wrong check, it was a MISSING one: CQLITE_PERF_SYSCTL_EXTRA_DIRS was a
new seam consumer, and the canonicalizing validation added for the write path simply
never reached it. No behavioural case can catch that class, because the defect is a
path nobody thought to test. So this audit enumerates, FROM THE SOURCE, every function
that dereferences a seam variable and requires each one to route through the
containment family (`perf_capability_sandbox_*` / `perf_capability_path_within`) — with
ONE named, justified allowlist entry. A future entry point that reads a seam without
the gate FAILS here, and joining the allowlist is a visible, reviewable act.
(rationale condensed; full reasoning in the commit history for #3261.)
```

### two-representations-because-the-two-matches-wa

```
TWO representations, because the two matches want different things (roborev round 6, Low).
`code`  — comments stripped only. The SEAM match needs this: a seam is a VARIABLE REFERENCE
and legitimately appears inside double quotes ("${CQLITE_PERF_SYSCTL_DIR:-}"), so
stripping quoted spans would hide real consumers and silently shrink the census.
`codeq` — comments AND quoted spans stripped. The GATE match needs this: a gate CALL is a
command, never a string, so matching inside quotes is what made the advertised
"command position" claim false — swapping a real call for a string that merely
mentions its name kept the audit green.
Quoted spans are removed before comments so a # inside a stripped string cannot truncate the
line. The single quote is written \047: this awk program sits inside a shell single-quoted
string and cannot contain a literal one.
```

### 1f-ii-the-same-audit-for-the-binaries-the-guar

```
1f-ii. THE SAME AUDIT FOR THE BINARIES THE GUARD AUTHORIZES (issue #3261 AC4). AC4 was the
EIGHTH escape from this family and the first about an EXECUTABLE rather than a path:
`CQLITE_PERF_TEST_PRIV_DIR=/usr` and a symlink-to-real-`sudo` inside a declared shim
dir both passed a textual check, so a privileged test-mode bootstrap could run the
host's real `sysctl --system`. Paths and executables are the same problem — a NAME is
not a DESTINATION — so they get the same STRUCTURAL treatment: every function that
resolves a privileged tool must route through the containment family, or be
allowlisted by name with a reason. Floor + explicit expectations, same as above.
Same de-vacuuming as the seam audit above: comments stripped, gate matched in a command position.
```

### two-representations-because-the-two-matches-wa

```
TWO representations, because the two matches want different things (roborev round 6, Low).
`code`  — comments stripped only. The SEAM match needs this: a seam is a VARIABLE REFERENCE
and legitimately appears inside double quotes ("${CQLITE_PERF_SYSCTL_DIR:-}"), so
stripping quoted spans would hide real consumers and silently shrink the census.
`codeq` — comments AND quoted spans stripped. The GATE match needs this: a gate CALL is a
command, never a string, so matching inside quotes is what made the advertised
"command position" claim false — swapping a real call for a string that merely
mentions its name kept the audit green.
Quoted spans are removed before comments so a # inside a stripped string cannot truncate the
line. The single quote is written \047: this awk program sits inside a shell single-quoted
string and cannot contain a literal one.
```

### and-the-newline-basename-case-now-actually-ex

```
...and the NEWLINE-BASENAME case, now actually EXERCISED (roborev round 27, Low). This block used to
assign cs_nl and then write a different, ordinary filename, so the line-oriented edge case it claimed
to cover never ran — a test asserting coverage it did not provide, which is the exact shape this suite
exists to catch elsewhere. The scan emits one entry per line, so a basename containing a newline could
split one competitor into two reported lines; it must fail closed and inject no extra lines.
ISOLATED DIRECTORY, and a REQUIRED nonzero status (roborev round 28, Low). My round-27 repair of this
case was itself vacuous twice over: 00-host-link.conf stayed in $cs_dir and sorts BEFORE the
newline-named file, so the scan refused the symlink and never reached this subject; and the assertion
only fired on rc 0 PLUS a matched line, so a silent accept -- or a skip -- passed. Its own isolated
directory removes the ordering dependency, and the refusal is now REQUIRED rather than merely allowed.
```

### and-the-spelling-is-not-the-destination-tmp-e

```
...and the SPELLING is not the destination. `/tmp/../etc/sysctl.d`, `<symlink-to-/etc>/…`
and — the R6-1 escape — `//etc/sysctl.d` (POSIX leaves two leading slashes
implementation-defined and `pwd -P` may PRESERVE them, while on Linux `//etc` IS `/etc`)
each passed the textual checks of an earlier round. There is no per-spelling check any
more: containment refuses all of them, plus every future spelling, for the SAME reason.
An UNENTERABLE path resolves to nothing and is refused too — a write target must exist.
```

### 1g-3261-a-name-is-not-a-destination-the-four

```
---- 1g. #3261: A NAME IS NOT A DESTINATION — the four remaining escapes ------------------
Positive containment closed the PATH SPELLINGS. These four are what containment of a
spelling still does not buy, and each is asserted BY ITS OWN OBSERVABLE CONSEQUENCE (a
followed write, a fabricated /proc verdict, a refused legitimate file, a real privileged
tool), never by an rc alone — this guard has several refusals and an rc-only check would let
the wrong one satisfy the case.
```

### 1g-i-ac1-high-directory-containment-is-not-wri

```
1g-i. AC1 (High) — DIRECTORY containment is not WRITE-TARGET containment. `tee <path>` opens
O_WRONLY|O_CREAT|O_TRUNC and FOLLOWS a symlink, so a symlink at the managed basename
inside a perfectly-contained directory pointed the privileged write at the LINK'S
TARGET — anywhere on the box. A contained directory says nothing about where its
entries point. Two independent requirements, both asserted:
* anything that merely NAMES the write target REFUSES (rc 1, empty, loud);
* the WRITE ITSELF replaces the directory ENTRY (rename), so a symlink planted in
the window between the check and the write is replaced, not written through.
(rationale condensed; full reasoning in the commit history for #3261.)
```

### and-the-staging-entry-is-unpredictable-create

```
...and the STAGING entry is UNPREDICTABLE, created by `mktemp` (roborev finding 1 on #3261 — the
NINTH escape, same shape as the other eight: a NAME trusted instead of a DESTINATION). A fixed
staging path that is checked, cleared and only THEN opened by a privileged `tee` is a TOCTOU
window: anyone who can create entries in the directory re-plants that KNOWN name as a symlink
between the verify and the open, and root follows it. Two asserts, because neither alone is
enough — a behavioural one (the previously-predictable name is planted as a symlink at a victim
file and must be left strictly alone) and a structural one (unpredictability is a property of the
NAME, which is gone by the time the write succeeds, so the source is the only place to see it).
```

### and-an-unsupported-host-is-reported-as-rc-2-d

```
...and an UNSUPPORTED HOST is reported as rc 2, distinct from rc 1 REFUSED (roborev rounds 16-17).
The staged install needs GNU `stat -c` and `mv -T`; bootstrap gates the perf section on
PLATFORM=linux, which is NOT the same as GNU, so a musl/busybox Linux host used to die on a raw tool
error. The tools are exercised INSIDE the privileged shell (sudo applies its own secure_path, so a
caller-side probe can check a different binary than the one that will run) and `mv -T` is EXERCISED
rather than grepped out of --help. The all-GNU control is what stops this passing by refusing always.
```

### and-an-extra-dirs-value-whose-first-line-is-v

```
...and an EXTRA_DIRS value whose FIRST LINE is VALID must still be refused (roborev round 31, Medium).
`read` consumes only the first line, so a value like "<contained-dir>\n/etc/sysctl.d" previously SUCCEEDED
while silently discarding the remainder -- the scan then reported on an incomplete set, which is the
falsely-reassuring answer the diagnostic exists to prevent. Round 3 validated the SPLIT ENTRIES and never
the value being split, so a newline HID entries rather than forging one. The baseline runs first, without
the newline, and must SUCCEED -- otherwise the refusal proves nothing.
```

### and-a-symlinked-control-file-inside-a-contain

```
...and a SYMLINKED CONTROL FILE inside a contained PROC_DIR must not be read (roborev round 25,
Medium). The directory gate proved the DIRECTORY contained and symlink-free and said nothing about its
ENTRIES, so `perf_event_paranoid` could be a link to the host file and the token would report a real or
attacker-chosen capability as if it came from the fixture. Same directory-is-not-its-entries lesson as
AC1, on the read path. The CONTROL is the identical tree with a REAL file, so the refusal cannot be
passing for an unrelated reason.
```

### and-line-safety-must-be-judged-on-the-origina

```
...and LINE-SAFETY MUST BE JUDGED ON THE ORIGINAL PATH, not the canonicalized one (roborev round
12, Medium). `$(cd -P -- "$p" && pwd -P)` STRIPS trailing newlines, so a directory whose name ends
in LF used to pass: the check only ever saw the stripped form, while every later caller emitted the
ORIGINAL spelling and split the one-per-line search path in two. Round 3 added the CR/LF guard for
exactly that split; it was running too late to see it. Both variants are pinned — a directory whose
name ends in LF, and a file whose PARENT ends in LF — because they canonicalize by different routes.
```

### baseline-first-without-the-newline-file-and-it

```
BASELINE FIRST, WITHOUT the newline file, and its status REQUIRED (roborev round 30, Low). My previous
version ran the "ordinary" baseline while the newline-named file was ALREADY present, making it identical
to the refusal case, and then discarded its status with `|| true` — so the negative control controlled
nothing. Three iterations of this one case have now been vacuous in a different way each time; the
pattern in my own work is that I fix the assertion and forget to re-check that it can still reach its
subject. Ordinary file only -> MUST scan (rc 0). Then add the newline file -> MUST fail closed.
```

### and-a-failing-content-generator-must-never-lo

```
...and a FAILING CONTENT GENERATOR must never look like success (roborev round 9, Medium). Both
call sites used to lose the generator's status: dropin_current ran a trailing sentinel `printf`
whose rc replaced it, so against an EMPTY file the compare was "X" == "X" and reported the drop-in
ALREADY CURRENT; dropin_install piped the generator into the privileged shell, so the pipeline's rc
was the last command's and a failure only surfaced if the CALLER had `pipefail`. Both are vacuous
positives from an unmeasured state. No GNU-only tooling is exercised here: each must fail BEFORE
any privileged command runs, which is the property under test.
```

### 1g-iii-ac3-low-a-strictly-contained-file-was-w

```
1g-iii. AC3 (Low) — a STRICTLY CONTAINED file was wrongly REFUSED. The file variant judged
its PARENT with the strict-containment predicate, so `<root>/sysctl.conf` failed:
the parent IS the root, and a root is not strictly inside itself. The judged path
must be <canonical parent>/<basename>, which IS strictly inside. A guard that
refuses legitimate input is the guard people learn to work around, so this is a
correctness case, not a convenience.
```

### 1c-iv-the-seam-list-is-complete-by-census-robo

```
1c-iv. THE SEAM LIST IS COMPLETE, BY CENSUS (roborev round 32, Medium x2).
(full rationale: fleet-runbook.md, perf seam containment, seam-list-completeness)
perf_capability_seam_set named CQLITE_PERF_PROC_DIR and CQLITE_PERF_SYSCTL_DIR only, while the
file had grown three more test-only seams (TEST_SANDBOX, SYSCTL_EXTRA_DIRS, TEST_PRIV_DIR). Any
of those exported WITHOUT the marker sailed through the env guard, which is the marker-less
refusal failing open -- the same "denylist of names" shape this whole issue exists to close, and
my own doing: the round-6 audit policed WHICH FUNCTIONS may read a seam and never WHICH SEAMS the
list must name, so it could not see an omission. This audit is the other direction, and it is a
CENSUS rather than a hand-kept list: every CQLITE_PERF_* name the library reads must be named by
seam_set, minus the marker itself (which cannot gate its own absence). Adding a seam without
listing it now FAILS here instead of silently widening the production surface.
```

### 1c-v-the-two-containment-paths-agree-about-a-s

```
1c-v. THE TWO CONTAINMENT PATHS AGREE ABOUT A SYMLINKED SANDBOX ROOT (roborev round 32, Medium).
(full rationale: fleet-runbook.md, perf seam containment, symlinked-sandbox-root)
sandbox_root_into advertised a "canonically spelled" root but never tested for symlinked
components. MEASURED on the same root and child: the fork-free perf_capability_sandbox_ok
returned 1 while the RESOLVING perf_capability_sandbox_ok_resolved returned 0 -- read and write
disagreeing about one sandbox, the same defect class as round 31's trailing-slash split.
Rejecting (not canonicalizing) is the only fix available here: sandbox_root_into is in the closed
fork-free audit set, and canonicalizing needs cd -P/pwd -P, i.e. a forked subshell.
THE ASSERTION IS AGREEMENT, not merely refusal: my first draft of this case asked only whether
the fork-free path refused, which it already did, so it passed with the defect fully present.
Both paths are therefore driven over the same fixtures, and the canonical control requires both
to ACCEPT -- a rule that refused everything would fail here just as loudly as one that accepts.
```

### b4

```

WHY THIS FILE EXISTS. Agent/worker images ship `kernel.perf_event_paranoid = 4` and set
it NOWHERE in /etc/sysctl.conf or /etc/sysctl.d — so every profiling run starts from a
hard EACCES whose help text ("access limited") reads like a CAPABILITY verdict when it is
a PERMISSION verdict. That has already cost two measurement cycles. The same three-line
incantation was then copy-pasted into ad-hoc harnesses; it now lives here, is git-pinned,
and is asserted by the gate's tooling-tests.

WHY -1 AND NOT 1. perf_event_paranoid is CUMULATIVE — higher is MORE restrictive and
each level keeps the ones below it: `>= 3` (an extra Debian/Ubuntu level) denies ALL
unprivileged perf use, which is why the images' `4` kills even a plain `perf stat`;
`>= 2` no kernel profiling; `>= 1` no CPU-WIDE access, which is exactly what
`perf stat -C <cpu>` needs; `>= 0` no raw tracepoints; `-1` (almost) everything, and the
perf mlock limit lifted too. CQLite's doctrine mandates per-CPU collection, so `1` is
not "almost right", it is a hard denial. `kernel.kptr_restrict = 0` is a SEPARATE control
for kernel SYMBOL resolution — without it kernel frames are unresolved addresses, a
SILENT attribution loss rather than an error. Same rationale in the drop-in's own bytes.

SECURITY POSTURE. A deliberate loosening, appropriate for DEDICATED SINGLE-TENANT
measurement/agent boxes. Never apply it to a shared or multi-tenant host. See
docs/development/fleet-runbook.md. BPF IS A DIFFERENT PERMISSION: a permissive
perf_event_paranoid does NOT grant BPF map creation — bpftrace/bcc collectors still need
sudo (#3217 finding).

Sourceable AND executable. Source it ONCE (the gate does, at script scope) and call the
functions; a per-use re-source re-reads 300+ lines for nothing. Sourcing has NO side
effects: this file only defines `perf_capability_*` functions plus the four
`PERF_CAPABILITY_*` constants (nothing runs, no shell options are changed, no variables
outside those namespaces are touched). Every function is `set -u` safe.

Usage when executed: `bash scripts/perf-capability.sh --help` (the modes are listed by
perf_capability_usage below, so they are not duplicated here).

```

### perf-capability-dropin-install-priv-cmd-write

```
perf_capability_dropin_install [<priv-cmd>...]: write the managed drop-in as an ATOMIC
DIRECTORY-ENTRY REPLACEMENT, so a pre-existing symlink at the managed name is REPLACED, never
FOLLOWED (issue #3261 AC1). argv is the privilege prefix (empty when already root); rc 0 iff the
managed bytes are in place at the managed path afterwards, verified by re-reading the file.

Content goes to a fresh staging entry in the ALREADY-VALIDATED directory, then `mv -fT` —
rename(2), which replaces the NAME and never dereferences the destination. Same directory, so the
rename is same-filesystem and atomic.

WHAT MAKES THIS SAFE IS THE PRECONDITION, NOT THE STAGING MECHANICS. Three successive fixes here
were each defended with a claim that proved FALSE, so the reasoning is recorded rather than the
conclusion alone (full history: #3261, roborev rounds 1-3):
* a FIXED staging name, checked-then-opened, claimed safe because the race "cannot happen". It
could: anyone able to create entries in the directory could re-plant that known name as a
symlink between the check and the privileged open.
* `mktemp` (O_CREAT|O_EXCL, 6 random chars, created under the SAME privilege that writes) closed
the CREATE race — but mktemp returns a NAME and each later step REOPENS it, so the window moved
rather than closing. A pid suffix would not have helped either; a pid is predictable.
* grouping every step into ONE privileged `sh -c` was then defended with a claim THIS COMMENT
ITSELF MADE AND WHICH IS FALSE: that no unprivileged process is scheduled between the steps.
`sh -c` gives SEQUENCING WITHIN ONE PROCESS, never MUTUAL EXCLUSION against other processes,
which run concurrently on other CPUs regardless of how we group our own commands. Consolidation
is kept — it removes needless windows — but it is NOT what makes this safe.
* what closes the class: REMOVE THE ATTACKER'S PRECONDITION. Every step of the race needs the
ability to create or replace entries in the target directory, so the install REFUSES a target
directory that anyone less privileged than the writer can write — it must be owned by the
identity performing the privileged write and be neither group- nor world-writable. There is
then no actor to race against, whatever the timing.
The ownership/mode test runs INSIDE the privileged shell against `id -u` of that shell, so it tests
the identity that will actually write (root in production, the shim under test mode) rather than
whoever invoked us. Undeterminable ownership or mode is a REFUSAL, not an assumption. Deliberately
conservative: group-writable is refused even with the sticky bit, because "arguably safe" is what
already cost this function three review rounds.
`chmod 0644` after the write is load-bearing: `mktemp` creates 0600, and the idempotency compare
runs from an UNPRIVILEGED bootstrap process that could not read a root-owned 0600 file — every
later run would see "not current" and rewrite. The old `tee` got 0644 from root's umask.
The staging name begins with `.` and does not end in `.conf`, so the competing-file scan (which
globs `*.conf`) can never mistake it for a rival drop-in.
GNU-COREUTILS DEPENDENCY, STATED EXACTLY: `mv -fT` and `stat -c` are GNU-only. The PRODUCTION
path is genuinely gated — bootstrap reaches this function only when PLATFORM=linux (set at :85,
branch at :412, PERF_SECTION_OK initialised to 0 at :405 so no ambient export can steer it).
NOT gated: scripts/tests/test_perf_capability.sh calls this DIRECTLY, so its staged-install cases
are capability-probed and COUNTED-skipped off GNU (roborev round 5). Neither portability guard in
the repo scans this file, so nothing mechanically protects the gate; recorded, not papered over.
```

### every-seam-is-unset-per-iteration-not-just-the

```
EVERY seam is unset per iteration, not just the marker (roborev round 33, Low, and it was RIGHT).
perf-capability-test-lib.sh:156 EXPORTS CQLITE_PERF_TEST_SANDBOX suite-wide, so leaving it set made
seam_set answer true through the INHERITED variable no matter which seam was under test. It was
invisible for the worst possible reason: before the fix seam_set did not NAME that seam, so the loop
measured correctly and went red for exactly the three missing ones -- the very fix that turned it
green is what would have made it vacuous. A negative control confirmed the repair: with seam_set
reverted to its two-name form, this loop fails again on those three seams.
```

### b27

```

POSITIVE CONTAINMENT, NOT A LIST OF FORBIDDEN PLACES (review R6-1/R6-2). Four rounds each
closed ONE MORE SPELLING of "the production directory": the raw path (B3), a symlinked seam,
`..` (R5-1), then `//etc` (R6-1 — POSIX leaves two leading slashes implementation-defined,
`pwd -P` may PRESERVE them, and on Linux `//etc` IS `/etc`). A denylist over path spellings
cannot be completed — `.`, `..`, symlinks, `//`, trailing slashes, bind mounts,
`/proc/self/root/…` all name the same directory — and scattered prohibitions also let a NEW
entry point silently miss them (R6-2). So the rule is INVERTED and there is exactly ONE: a
seam is usable IFF it is strictly contained in the declared sandbox root. Every spelling of
"somewhere else", including every future one, fails that single check for the same reason.
TEST MODE HAS NO FALLBACK (R4-3). Under the marker the sandbox root and BOTH path seams are
MANDATORY. The earlier shape — marker set, seam unset, fall back to the real directory —
meant test mode could pass the env guard (sudo/sysctl absent, or present as declared shims)
and a subsequent root `--yes` run would `tee` the REAL /etc/sysctl.d, mutating the host from
a test run. "Hermetic" cannot be a claim that depends on a variable being set.
```

### perf-capability-seam-set-rc-0-iff-any-test-onl

```
perf_capability_seam_set: rc 0 iff ANY test-only seam is non-empty. The ONE
seam reader outside the containment gate below, and deliberately so: it asks only
"was a seam handed to us at all" (for the marker-less refusal) and never uses the
VALUE as a path. The structural audit in test_perf_capability.sh allowlists it by
name, so a future function cannot join it silently.

EVERY non-marker seam MUST be listed here (roborev round 32, Medium). This named only PROC_DIR
and SYSCTL_DIR while the file had grown three more, so any of those exported WITHOUT the marker
passed the guard — the marker-less refusal failing OPEN, which is the same incomplete-list-of-
names shape this whole file exists to avoid. The round-6 audit that was supposed to protect this
policed WHICH FUNCTIONS may read a seam, never WHICH SEAMS this list must name, so it was blind
to an omission by construction. The completeness direction is now enforced by CENSUS in
test_perf_capability.sh (1c-iv): every CQLITE_PERF_* name the library reads, minus the marker
itself, must appear below — so adding a seam without listing it here FAILS the suite.
```

### 1c-vii-the-probe-deletes-nothing-it-did-not-cr

```
1c-vii. THE PROBE DELETES NOTHING IT DID NOT CREATE, and a WORKING mv that FAILS is not an
unsupported host (roborev round 34: Medium + Low). The probe used to rm -f its predictable
".perfcap-probe.<pid>" names before proving absence, so a stale entry -- or, after PID reuse, an
unrelated file -- was silently deleted under privilege; the round-21 absence proof closed the
symlink-truncation hazard but left the delete itself. And every rename failure was reported as
rc 2 UNSUPPORTED, so a filesystem or mount-policy failure suppressed the retry remedy precisely
where retrying is right. These cases need GNU staging, so they ride the same capability gate.
```

### the-production-paths-below-are-hardcoded-liter

```
The PRODUCTION paths below are HARDCODED LITERALS (/etc/sysctl.d, /proc/sys/kernel) because
bootstrap installs the drop-in through the STAGED installer below (mktemp + atomic rename, no
`tee <path>`): were that path env-derived, one stray
export (say CQLITE_PERF_SYSCTL_DIR=/etc/sudoers.d) would make ROOT write an
attacker/accident-chosen file while the real drop-in was never installed — and an unparsable
sudoers entry can wedge `sudo` outright. Likewise an env-chosen /proc stand-in would let a
paranoid-4 box print a FABRICATED "verified" verdict. So the seams take effect ONLY under
the explicit marker, and the marker is itself hermetic: with it set, a REAL `sudo`/`sysctl`
reachable on PATH is a hard refusal (the suite PATH-shims both and declares the shim
directory), so test mode can never reach a real privileged tool.
CQLITE_PERF_TEST_MODE=1     the marker; without it every seam below is INERT
CQLITE_PERF_TEST_SANDBOX    THE SANDBOX ROOT — the one absolute directory every other
seam must be provably INSIDE (test mode only)
```

### dropped-mech-asserts-the-mechanism-was-invoke

```
* "dropped:<mech>" asserts the mechanism was INVOKED; it cannot prove the kernel changed
uid. Harmless by construction: the caller's verdict is `token = ok` AND the functional
pass, and a box whose /proc says ok IS profileable unprivileged, so a mislabelled drop
cannot manufacture a capability that is absent.
* the READ-side containment check is SYNTACTIC (fork-free, gate contract) while every
write / host-config read canonicalizes — SAME predicate, different input form. The read
side judges the spelling, so a symlinked ancestor INSIDE the sandbox could still steer a
test-mode /proc STAND-IN read. Bounded by that path's whole contract: the seams are
honoured only under the test marker, which is never set in production, and nothing there
writes — so the worst case is a read of a caller-chosen file reported as
`absent`/`unknown`, never a fabricated capability.

```

### one-gate-positive-sandbox-containment-review

```
---- ONE GATE: POSITIVE SANDBOX CONTAINMENT (review R6-1/R6-2) --------------------
THE sandbox root is caller-declared (CQLITE_PERF_TEST_SANDBOX) and must PROVE itself: an
absolute, canonically spelled, existing directory carrying the stamp file below. The stamp is
what makes the declaration unforgeable by environment alone — a stray
CQLITE_PERF_TEST_SANDBOX=/etc cannot turn /etc into a sandbox, because the proof lives on the
FILESYSTEM and writing it into a system directory already needs the privilege this guard
protects. No denylist appears below: a path is usable because it is provably INSIDE the
sandbox, never because it failed to look like somewhere forbidden.

FIVE thin functions, ONE predicate — everything ends in perf_capability_path_within:
sandbox_root_into O        the declared root, validated; rc 1 + empty when unproven
path_within P R            THE predicate (below)
(rationale condensed; full reasoning in the commit history for #3261.)
```

### mv-t-is-exercised-here-after-the-ownership-pre

```
mv -T IS EXERCISED HERE, AFTER the ownership precondition and BEFORE the real staging entry.
Placement is deliberate on both sides. AFTER the precondition, because that is what establishes
no less-privileged actor can create entries in $d — which is precisely what makes a PREDICTABLE
probe name safe, so this does not need (and must not consume) mktemp. NOT consuming mktemp also
keeps it from pre-empting the staging entry checks below, which have their own cases.
-T SUPPORT IS QUERIED, NEVER INFERRED FROM A FAILED RENAME (roborev round 34, Low). Treating
every probe-rename failure as "no GNU mv" misdiagnosed filesystem errors, mount policy and
transient conditions as an unsupported HOST, which made bootstrap suppress the retry remedy in
exactly the cases where retrying is the right advice. Capability and outcome are now separate
questions: this asks whether -T exists, the rename below asks whether it works here.
```

### arguably-the-most-likely-one-since-installing

```
(arguably the most likely one, since installing the drop-in needs root). A root-run
functional check reported as "perf capability verified" is therefore a FALSE verification
of an unprofileable box: the failure mode the functional check exists to remove,
reintroduced through the privilege dimension. The property under test is "an UNPRIVILEGED
process can collect CPU-WIDE cycles", which a root-run probe cannot demonstrate — so the
probe DROPS PRIVILEGE when it can and SAYS SO when it cannot, and the caller then
subordinates the functional result to the identity-independent /proc token.

perf_capability_self_uid_into <outvar>: THIS process's uid, rc 0 ONLY when genuinely known
— `id -u` must EXIST, exit 0, and print a validated non-negative integer. rc 1 (<outvar>
emptied) means "identity unknown", which is NOT "unprivileged" (review R4-1). The previous
shape, `$(id -u 2>/dev/null || echo 1000)`, FAILED OPEN: a missing or broken `id` made a
ROOT process look unprivileged, so its root perf run was accepted as unprivileged evidence
and printed a false VERIFIED — the R3-1 defect, through the detector written to prevent it.
```

### test-mode-a-proven-sandbox-root-plus-both-seam

```
test mode: a PROVEN sandbox root plus BOTH seams RESOLVING
strictly inside it, plus every reachable sudo/sysctl inside
an absolute declared shim dir.
dropin_path rc 0            its directory came from `sysctl_dir`, i.e. RESOLVES inside the
sandbox — the single gate between the bytes and a root `tee`.
dropin_current rc 0         a BYTE-exact compare (trailing newlines included) against the
canonical content from a read that reached EOF — a
NUL-delimited read is NOT current (R5-3).
```

### no-symlinked-component-including-the-root-s-ow

```
NO SYMLINKED COMPONENT, including the root's own final component (roborev round 32, Medium).
Without this the function advertised a canonically spelled root while accepting one reached
THROUGH a symlink, and the two containment paths then disagreed about the identical root and
child: measured rc 1 from the fork-free sandbox_ok versus rc 0 from the resolving
sandbox_ok_resolved. One sandbox must not be both contained and not contained. REJECTING rather
than canonicalizing is forced, not preferred: this function is in the closed fork-free audit set
and canonicalizing would need cd -P plus pwd -P, i.e. a forked subshell. A root must be spelled
as its own destination -- the same rule the drop-in destination and the shim tools already obey.
```

### the-containment-predicate-and-the-only-place-a

```
THE containment predicate, and the only place a path is ever judged: rc 0 iff <path> is
absolute, canonically spelled (no `.`, `..` or `//` component — `//etc` IS `/etc`, R6-1),
free of CR/LF (so a contained path can never SERIALIZE into two entries, roborev round 3)
and STRICTLY inside <root>, with the `/` boundary explicit so `/tmp/sandboxevil` is NOT
inside `/tmp/sandbox`. An empty root refuses; it is never a wildcard.
The line check lives HERE, in the one predicate every entry point ends in, for the same reason
containment does: one choke point cannot be skipped by a future consumer.
```

### perf-capability-nosymlink-rc-0-iff-path-is-abs

```
perf_capability_nosymlink: rc 0 iff <path> is absolute and NO path component — the final one
INCLUDED — is a symlink. FORK-FREE: `[ -L ]` is a shell builtin test, so this is usable on the
gate's contractually fork-free token path, where `cd -P`/`pwd -P` (a subshell, i.e. a process)
is not.

WHY (issue #3261 AC2). Containment of a SPELLING is not containment of a DESTINATION. The
inversion to positive containment made the fork-free read check purely textual and thereby
(rationale condensed; full reasoning in the commit history for #3261.)
```

### the-file-variant-judged-as-canonical-parent-ba

```
The FILE variant. Judged as <CANONICAL PARENT>/<basename> (issue #3261 AC3): canonicalizing the
parent and asking whether THE PARENT is contained refused `<sandbox-root>/sysctl.conf`, because
the parent there IS the root and a root is not STRICTLY inside itself — a legitimate,
strictly-contained file rejected, which is how a guard teaches people to route around it. The
assembled path is the thing being authorized, so it is the thing judged.
The final component may not be a SYMLINK (the AC1 lesson, here on a read whose CONTENTS are
consumed): a symlinked `sysctl.conf` inside the sandbox would feed the competing-file scan the
host's real configuration.
```

### the-into-outvar-convention-the-gate-s-summary

```
THE `*_into <outvar>` CONVENTION. The gate's summary path calls the token chain below and
is contractually FREE — no external process AND no command substitution (each `$( )` forks
a subshell, which is a process too). A function that answers on stdout therefore CANNOT be
on that path: its caller must fork to read it. So every function the gate touches has an
`_into <outvar>` core assigning through a caller-named variable, and the stdout-printing
form is a thin wrapper for CLI/bootstrap ergonomics — the wrapper is the ONLY place a fork
is paid, and it is not on the gate's path. Assignment is `eval "$1=\$var"`, NOT a
(rationale condensed; full reasoning in the commit history for #3261.)
```

### normalise-before-the-gate-exactly-as-the-sandb

```
NORMALISE BEFORE THE GATE, exactly as the sandbox root does (roborev round 33, Low). Stripping
after the containment check left the two halves inconsistent: the ROOT accepted a "//" trailing
spelling while PROC_DIR refused the identical one, because the gate saw the raw value. Callers
join with "/$name", so an unnormalised trailing slash also built a "//" entry path that the
entry-level check then refused -- surfacing as the capability verdict "absent" rather than as a
refusal, i.e. a mere spelling became a definite negative claim about the host, in the one
function the gate's perf= token comes from. INTERIOR "//" stays refused: only trailing
separators are normalised, and a non-canonical spelling is still not a destination.
```

### trailing-slashes-are-stripped-before-any-check

```
TRAILING SLASHES ARE STRIPPED BEFORE ANY CHECK OR PATH CONSTRUCTION (roborev round 10, Low).
`[ -L "$d" ]` FOLLOWS a trailing slash: for a symlinked directory `link`, `[ -L link ]` is true
but `[ -L link/ ]` and `[ -L link// ]` are FALSE, so the destination-symlink refusal this
function explicitly promises could be walked past with one extra character. Stripping is the
right shape here and NOT another spelling denylist: normalising the input to ONE canonical form
makes the affirmative check total, whereas enumerating bad spellings is the unbounded game this
family lost eleven times. The length guard keeps `/` itself from becoming the empty string —
a root destination then fails the ownership/writability precondition on its own merits rather
than by accident.
```

### content-is-generated-and-checked-before-any-pr

```
CONTENT IS GENERATED AND CHECKED **BEFORE** ANY PRIVILEGED COMMAND RUNS (roborev round 9,
Medium). This used to pipe the generator straight into the privileged shell, so the pipeline's
status was the LAST command's and a failed generator was invisible unless the CALLER happened to
have `pipefail` set — a correctness property no library function should delegate to its caller.
Worse, the privileged write would already have started on empty or partial content. Generating
first means a generator failure returns before privilege is acquired at all. Same sentinel trick
as dropin_current, for the same reason: `$( )` strips trailing newlines, and the drop-in's final
newline is part of the canonical bytes the idempotency compare comes back for.
```

### zero-pad-before-taking-the-last-three-digits-s

```
ZERO-PAD BEFORE TAKING THE LAST THREE DIGITS. `stat -c %a` drops leading zeros, so mode 0033
arrives as "33" — and `${dmode%???}` cannot match a 2-character string, leaving `dperm` EMPTY,
matching none of the write-bit patterns below, and PASSING a group- AND world-writable
directory. That was a real bypass of this very precondition (roborev round 5, High), and it
survived a hand audit that reasoned about the 3- and 4-digit cases and never considered a
SHORTER one. The suffix-strip idiom is only safe once the string is known to be long enough,
so the padding is not cosmetic: it is what makes the check below total.
```

