plugins {
    java
}

group = "com.rustyrazorblade.cqlite"
version = "0.11.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

// Latest Trino at time of writing. The SPI is `compileOnly` — Trino provides it
// at runtime from the engine classpath; bundling it would clash.
val trinoVersion = "481"
val arrowVersion = "18.1.0"
val jacksonVersion = "2.18.2"

dependencies {
    compileOnly("io.trino:trino-spi:$trinoVersion")

    implementation("org.apache.arrow:flight-core:$arrowVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")

    testImplementation("io.trino:trino-spi:$trinoVersion")
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

// Arrow on JDK 25 needs the foreign-memory module opened for off-heap access.
tasks.withType<Test>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED")
}
