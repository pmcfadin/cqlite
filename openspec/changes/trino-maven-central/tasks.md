## 1. Package namespace rename (`in.mcfad.cqlite.*`)

- [x] 1.1 Move source dirs `trino-connector/src/{main,test}/java/com/rustyrazorblade/cqlite/...`
      → `.../in/mcfad/cqlite/...` (use `git mv` to preserve history). Surface = source tree layout.
- [x] 1.2 Rewrite `package` + `import` declarations `com.rustyrazorblade.cqlite` → `in.mcfad.cqlite`
      across all 38 `.java` files. Surface = compiled package names.
- [x] 1.3 Update the SPI descriptor
      `src/main/resources/META-INF/services/io.trino.spi.Plugin` →
      `in.mcfad.cqlite.flight.CqliteFlightPlugin`. Surface = Trino plugin discovery.
- [x] 1.4 Verify rename E2E: `./gradlew test installPlugin` green; `build/plugin/cqlite_flight/`
      holds the jar; `grep -r com.rustyrazorblade src` returns nothing. Surface = `installPlugin`.

## 2. Gradle publishing + signing config (`build.gradle.kts`)

- [x] 2.1 Set `group = "in.mcfad"` (replace `com.rustyrazorblade.cqlite`); keep
      `rootProject.name = "cqlite-trino"` so artifactId is `cqlite-trino`. Surface = coordinates.
- [x] 2.2 Add the `com.vanniktech.maven.publish` plugin (Central Portal mode) — or raw
      `maven-publish` + `signing` with `withSourcesJar()`/`withJavadocJar()` per design.md.
      Surface = the publication + sources/javadoc jars.
- [x] 2.3 Author the full POM metadata: name, description, url, license (project license),
      developers, SCM (`scm:git:https://github.com/pmcfadin/cqlite.git`). Surface = generated POM.
- [x] 2.4 Wire GPG signing from in-memory keys (`SIGNING_KEY` / `SIGNING_PASSWORD`), applied only
      when a key is configured so the secret-free local build still succeeds. Surface = `.asc` files.
- [x] 2.5 Keep `trino-spi` `compileOnly` and assert it is absent from the published POM's runtime
      deps; confirm `flight-core` + `jackson-databind` are present. Surface = generated POM scopes.
- [x] 2.6 Derive `version` from the `version` project property (`-Pversion=`), falling back to a
      defined local dev version when absent. Surface = artifact version.
- [x] 2.7 Verify publication E2E (no real secrets):
      `./gradlew publishToMavenLocal -Pversion=0.13.0` yields
      `~/.m2/.../in/mcfad/cqlite-trino/0.13.0/` with main + `-sources` + `-javadoc` jars and a
      complete POM. Surface = `publishToMavenLocal`.

## 3. Tag-triggered, secret-gated release workflow

- [x] 3.1 Add `.github/workflows/trino-publish.yml` on `push: tags: ['v*']`: set up JDK 25,
      derive the version from `GITHUB_REF_NAME`, run the Gradle publish to Maven Central.
      Surface = the workflow file / CI run.
- [x] 3.2 Gate the publish on secret presence via a guard step → job/step output `if:` (secrets
      are not addressable in a job-level `if:` directly); log a loud `::notice::` and succeed when
      absent. Surface = the gating step.
- [x] 3.3 Confirm the workflow does not run the publish on non-tag events. Surface = trigger config.

## 4. Consumer docs

- [x] 4.1 Update `trino-connector/README.md` with the published coordinates
      `in.mcfad:cqlite-trino:<version>` and how to assemble the plugin directory from the published
      artifact (a Trino plugin is a dir of jars, mirror `installPlugin`). Surface = README.

## 5. Doctrine + verification

- [x] 5.1 Note in the PR / README that the Rust `scripts/agent-gate.sh` does not cover this Java
      project; the verifying runs are the Gradle commands in tasks 1.4 and 2.7. Paste those outputs.
- [x] 5.2 Rust `scripts/agent-gate.sh` is N/A: `git diff origin/main...HEAD` touches **zero** `.rs`
      / `Cargo.*` files (Java/Gradle/CI only), so the Rust gate cannot regress from this change. The
      verifying runs are the Gradle commands (tasks 1.4 + 2.7), both green; the PR's own
      `trino-connector-ci.yml` re-runs `./gradlew test installPlugin` as the CI gate.
- [x] 5.3 C — `spec-auditor` anchored to `specs/**`: **PASS**. All 5 requirements / 10 scenarios
      `satisfied` with public-surface evidence (auditor re-ran the Gradle builds + verified signing).
- [x] 5.4 Code review: roborev is **environment-blocked** (its backend agents fail health check
      here). Substituted an independent opus diff-review → **CLEAN**, no blocking findings (secret
      gating, env↔plugin wiring, version derivation, POM scoping, rename completeness all verified).
