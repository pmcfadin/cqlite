## Why

The Arrow Flight server (`cqlite-flight`) and the Trino connector (`cqlite-trino`) shipped in v0.12
and are a headline capability ("query SSTables as a federated source"), but on the **published docs
site** (https://pmcfadin.github.io/cqlite/) they have **no dedicated user-facing page**. They surface
only *incidentally* — a mention in `use-cases/sidecar-lakehouse.md`, a roadmap line, and a
`read-surfaces-and-freshness.md` reference.

The `website/src/content/docs/user-docs/` section has first-class pages for the CLI, Python, Node,
output formats, write support, and observability — but nothing for Flight/Trino. Someone browsing the
site to adopt the feature cannot find "how do I run the Flight server and wire up Trino"; the only
real material lives in the repo crate READMEs (`cqlite-flight/README.md`, `trino-connector/README.md`),
which a site visitor does not see.

- **Milestone:** 0.14. **Design-driven** (docs / UX surface — there is no Cassandra SSTable format
  oracle here). Routed through `flow-activate` (OpenSpec).
- Adds a new `flight-trino-user-docs` capability.
- The engineering **decisions** (the compaction-on-read model, the ticket contract, the container
  image, the Sidecar discovery + snapshot lifecycle, catalog config) are already resolved and
  documented in the crate READMEs. This change is a **user-facing distillation** onto the published
  site — not new engineering, and it changes no product code or API.

## What Changes

- **A new user-docs page** at `website/src/content/docs/user-docs/flight-trino.md` with valid
  Starlight frontmatter (title, description, sidebar label + order). The `user-docs/` sidebar group
  autogenerates from the directory, so the page appears in the site nav automatically.
- **Content**: what the Flight server is and the co-located compaction-on-read model (SSTables merged
  on the fly, originals untouched; flushed-SSTables-only; snapshot-aware); running the published
  container (`ghcr.io/pmcfadin/cqlite-flight`) with a `docker run` example against a read-only mount;
  the Flight ticket / predicate / projection contract with a runnable PyArrow client example; and the
  Trino connector — Sidecar node/token-range discovery, one-split-per-replica model, plugin install,
  and catalog config incl. `read-mode` snapshot-vs-live.
- **Discovery wiring**: at least one existing page links to the new page (the
  `use-cases/sidecar-lakehouse.md` cross-reference and/or the site nav) so it is reachable, not
  orphaned. The page links back to the authoritative crate READMEs for deep detail rather than
  duplicating it wholesale.

## Capabilities

### Added Capabilities
- `flight-trino-user-docs`: a first-class, discoverable, build-validated user-docs page on the
  published site covering the Arrow Flight server and the Trino connector — what they are, how to run
  the container, the client/ticket API, and how the Trino connector discovers and installs.

## Impact

- **Added:** `website/src/content/docs/user-docs/flight-trino.md`.
- **Modified (minimal):** a cross-link from `website/src/content/docs/user-docs/use-cases/sidecar-lakehouse.md`
  (and/or `README.md`'s Arrow Flight section) pointing at the new page.
- **No `src/` / cqlite-core / cqlite-flight / trino-connector source changes.** `scripts/agent-gate.sh`
  (the Rust gate) does not exercise the website; the correctness gate for this change is the site
  build + link validator: `cd website && npm run build` must pass, including `starlightLinksValidator`
  (fails on any broken internal link/anchor).
- **No-heuristics mandate / memory budget / public binding surfaces:** unaffected (docs only).

## Non-goals

- **No product, API, CLI, or connector behavior changes** — this is documentation only. If writing the
  page reveals a README inaccuracy or a genuine product gap, that is filed as its own issue, not fixed
  here.
- **No re-derivation of the design.** The crate READMEs and `docs/flight-trino/PLAN.md` remain the
  authoritative source; this page distills them and links to them.
- **No new API-reference / rustdoc generation** for the Flight crate (that is the separate
  `api-docs.yml` subtree).
- **No changes to the container publish pipeline, the Maven artifact, or the Sidecar** — those are
  covered by their own capabilities (`flight-container-distribution`, `trino-connector-release`).
- **Not an exhaustive operator runbook** — the page is an adoption on-ramp with the key config and a
  working example; deep tuning stays in the READMEs it links to.
