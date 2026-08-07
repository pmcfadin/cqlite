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
bash scripts/flow/claim.sh smoke               # preflight: prove origin accepts refs/claims/* (see below)
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
`refs/claims/smoke-<nonce>` ref to confirm the remote permits it (`SMOKE-OK` = good). This is
**verified working on github.com/pmcfadin/cqlite** (2026-07-17). Run it **once when adopting a new
remote or host** — a managed Git host that restricts custom ref namespaces would make the whole
claim mechanism unusable, and that must be caught before the fleet relies on it. **Non-unique
hostnames:** the claim holder identity is `hostname -s`; on a fleet of cloud images/containers/cloned
VMs that report the *same* short hostname, export a UNIQUE `CLAIM_MACHINE` per box (else two machines
share one identity and each treats the other's claim as its own).

Sanity check: `bash scripts/agent-gate.sh --lite` should pass in ~1–5 min, and the SUMMARY's
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
one worker per machine, no other tenants, no untrusted logins). It **must not** be applied to shared
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
  linger, or disk is low — it waits, it never spins. A flock makes a second supervisor on the same
  machine refuse to start. (The Claude probe keys on the supervisor's own `-p … --agent flow-lead`
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
  open PR → Status → Ready, work preserved on the branch, traceable comment; issue #2089). The
  supervisor also stamps a machine-scoped claim ref `refs/machine-claims/<machine>` that the
  `project-board-sync` 30-min cron's `reap-claims` job reaps on the SAME predicate server-side (age >
  4h AND no open PR AND, for a local claim, PID-dead) — so a supervisor that dies overnight gets its
  claim reaped by CI without waiting for a human to run flow-board (issue #2655)
- **Ready but branch already on origin** → parked-by-design (e.g. spec approved, awaiting a
  team) — pickup is *resume that branch*, never a fresh claim

## Recovery scenarios (all safe by construction)

| Scenario | What to do |
|---|---|
| Laptop lid closed mid-issue | Nothing. Commits are on the origin branch. Reopen and say `implement <N>` — it resumes from the worktree. |
| Session feels degraded / bloated | Kill it, start fresh. Board + disk are the state; the new session rehydrates in one board read. |
| Board unreachable (auth/scope error) | The session STOPS by design (labels are decorative, never a dispatch source). Fix `gh auth refresh -s project` and restart. If the scope is already present and `gh project` still fails for `read:org`, that is the #2942 delta — use the `updateProjectV2ItemFieldValue` GraphQL mutation for board writes, or widen the token with `-s read:org`. |
| `fatal: could not read Username` on any push | git has no credentials even though `gh` does (#2942). The claim protocol pushes with plain git, so it is fully broken until fixed: `gh auth setup-git`, or `bash scripts/bootstrap-agent-machine.sh --yes`. A claim attempt on such a box reports `reason=auth`, not a retryable transient. |
| Gate seems hung | It's probably queued: look for `waiting for gate slot (N in use)…`. Queued ≠ hung. |
| Green SUMMARY but parity lines say SKIP | Datasets missing on that machine — `fetch-datasets.sh`, export the root it prints, re-run. Probe an existing root with `fetch-datasets.sh --verify-only` (mutates nothing). The FULL gate FAILs CLOSED here (`missing-fixtures: FAIL-CLOSED (#2078)`) so it can't slip through; `--lite`/`--only` stay lenient. |
| `missing-schemas: FAIL-CLOSED (#3148)` | Either a committed `test-data/schemas/*.cql` is unreadable (broken checkout — `git restore --source=HEAD -- test-data/schemas`) or `CQLITE_SCHEMAS_ROOT` is set to a **relative** path (export an absolute one, or unset it). Never a corpus-layout problem: the schemas root is checkout-relative. No opt-out exists — do not look for one. |
| Two machines want the same issue | Impossible past the claim: the second claim-ref push is rejected server-side (non-fast-forward on the fixed-name ref, #2665); the loser sees `CLAIM LOST` and picks the next Ready item. |

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

