plugins {
    // Auto-provisions the Java 25 toolchain so the build is independent of the
    // host JDK (CI/dev may only have JDK 21).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "cqlite-trino"
