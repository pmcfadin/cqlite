Closes #2193
Closes #2290

## Problem

On JDK 17+, arrow-java's `MemoryUtil.<clinit>` requires the JVM flag
`--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`. Trino 481's stock
`jvm.config` does **not** carry it, and the k8s `trino-cqlite` kit did not add it. So the CQLite
Flight connector's first Arrow off-heap touch (`new RootAllocator()`) died in the static
initializer, and every `do_get` on the read path then failed far downstream with a cryptic
`Failed to read message` — with nothing pointing operators at the real cause. Adding the flag and
restarting Trino makes the whole read path work.

## Fix (one PR, three layers)

1. **k8s kit provisions the flag** — `easy-db-lab-kits/trino-cqlite/bin/reapply-plugin-patch.sh.template`
   now injects `JAVA_TOOL_OPTIONS=--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
   into both the `trino-coordinator` and `trino-worker` container specs, via the same always-run
   strategic-merge patch (`patch_deployment`). `JAVA_TOOL_OPTIONS` is additive (the JVM appends it to
   the launcher args) so it doesn't require reproducing/version-tracking the image's whole stock
   `jvm.config` the way a file-mount would; strategic merge keys `env` by name, so re-applying the
   fixed value is a no-op (idempotent).

2. **Connector fail-fast** — new `ArrowMemoryPreflight.verify()` runs once at connector construction
   (before the first `RootAllocator`). It probes Arrow memory init in a try-with-resources allocator
   (side-effect-free when the flag is present), and on a `MemoryUtil` init failure raises a Trino
   `CONFIGURATION_INVALID` error naming the **exact** missing flag and the `jvm.config` remedy.
   Unrelated errors pass through untouched (never masked).

3. **Docs** — a "Required JVM configuration" section in `trino-connector/README.md` and
   `website/src/content/docs/user-docs/flight-trino.md` documents the flag, why (arrow-java JDK17+
   module access for off-heap), and the fail-fast behavior.

## Why the docker e2e never caught this (the blind spot)

The docker e2e passes without the fix because `docker/docker-compose.yml` mounts
`docker/trino/jvm.config`, and that file already contained an add-opens flag (the looser
`...=ALL-UNNAMED` form). So local e2e exercised a Trino that always had the module opened, while the
kit-based deployment — and any other deployment shape that starts from Trino's stock `jvm.config` —
had no such flag and broke on the first frame. The e2e's own jvm.config masked the failure it was
supposed to guard. That is exactly why the **connector-side fail-fast probe (layer 2)** is the
durable protection: it fires at catalog load on *any* deployment shape, regardless of how (or
whether) `jvm.config` was provisioned, turning a cryptic read-time `Failed to read message` into an
actionable configuration error. (I also aligned `docker/trino/jvm.config` to the exact arrow-java
form so the docker stack, the kit, and the fail-fast message all name the identical flag.)

## Verification

- `cd trino-connector && ./gradlew test` — GREEN (314 tests, 0 failures), including the new
  `ArrowMemoryPreflightTest` (9 cases: message names the exact flag, classifier matches
  MemoryUtil-init failures by message and by stack frame, arrow-init → `CONFIGURATION_INVALID`,
  unrelated errors rethrown unchanged, a 2-node cause-cycle terminates without hang/misclassification,
  and `verify()` passes side-effect-free — forcing a 1-byte off-heap `ArrowBuf` allocation so the probe
  truly exercises the `MemoryUtil` path — when the flag is present in the Gradle test JVM).
- Kit template: `shellcheck` clean; jq patch validated to emit the correct `env` entry; idempotent
  by strategic-merge semantics.

## Review hardening (addressed in-branch)

- The preflight probe now forces a 1-byte off-heap `ArrowBuf` allocation, so it genuinely exercises the
  `MemoryUtil`/java.nio path a bare `RootAllocator` could skip.
- The kit **appends** to `JAVA_TOOL_OPTIONS` if-absent rather than clobbering an existing literal value,
  and when the var is sourced via `valueFrom`/`envFrom` it emits a loud actionable warning and skips the
  literal overwrite (the connector fail-fast probe is the backstop).
- The cause-chain walk is now bounded so any cyclic cause chain terminates.

Non-blocking follow-up nits (uninstall flag-strip; two additional test branches) are batched in #2296.
