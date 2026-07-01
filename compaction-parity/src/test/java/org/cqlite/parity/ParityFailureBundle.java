/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cqlite.parity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Assembles a scenario-id-keyed parity failure bundle for the compaction Java
 * harness (issue #1027, task 3.1/3.2) under the SHARED layout every parity lane
 * uploads:
 *
 * <pre>
 * &lt;root&gt;/parity-failures/&lt;tier&gt;/&lt;scenario_id&gt;/
 *   failure-artifact.json      # the uniform record ({@link ParityFailureArtifact})
 *   stdout.txt / stderr.txt
 *   diffs/                     # byte_for_byte: per-component byte/offset diff + checksums + inventory
 *                              #                (+ live_log = failing comparison stdout/stderr, nightly_docker)
 *                              # canonical_semantic: jsonl.diff + raw reference/candidate.jsonl
 *   repro/
 *     command.sh / INSTRUCTIONS.md
 *     inputs/fixtures.txt      # fixture path + dataset SHA256 (NO dataset copy)
 * </pre>
 *
 * <p>This complements the existing forensic dir written by {@link ParityArtifacts}
 * (which stays as the developer-facing per-scenario snapshot); the bundle here is
 * the CI-uploaded, manifest-joinable record. It reuses the same diff/checksum
 * bodies so the two never drift.
 *
 * <p>{@code <root>} is {@code -Dparity.failures.dir}; the Gradle build points it
 * at {@code compaction-parity/build/parity-failures} which the workflow uploads
 * (Wave 2b wired the {@code parity-failures/**} glob).
 */
final class ParityFailureBundle
{
    private final Path root;
    private final String scenarioId;
    private final String tier;
    private final String evidenceType;
    private final String lane;

    private String stdout = "";
    private String stderr = "";
    private final List<String> artifactsCompared = new ArrayList<>();
    private final List<DiffFile> diffFiles = new ArrayList<>();
    private final List<RawFile> rawFiles = new ArrayList<>();

    // Provenance
    private String cassandraVersion = "unknown";
    private String cassandraGitSha = "0000000";
    private Path fixturePath;
    private List<String> componentList = new ArrayList<>();
    private String commandLine = "";

    private ParityFailureBundle(Path root, ParityScenarioMap.Resolution res, String lane)
    {
        this.root = root;
        this.scenarioId = res.scenarioId;
        this.tier = res.tier;
        this.evidenceType = res.evidenceType;
        this.lane = lane;
    }

    /** A diff file that becomes both an on-disk artifact and a record {@code diffs[]} entry. */
    private static final class DiffFile
    {
        final String kind;      // FAILURE_ARTIFACT_KIND value
        final String fileName;  // relative under diffs/
        final String body;

        DiffFile(String kind, String fileName, String body)
        {
            this.kind = kind;
            this.fileName = fileName;
            this.body = body;
        }
    }

    /** A raw bundle file under diffs/ that is NOT a record {@code diffs[]} pointer. */
    private static final class RawFile
    {
        final String fileName;
        final String body;

        RawFile(String fileName, String body)
        {
            this.fileName = fileName;
            this.body = body;
        }
    }

    /** Start a bundle for {@code Class.method}, keyed by the resolved scenario id. */
    static ParityFailureBundle forMethod(String classDotMethod, boolean byteTier, String lane)
    {
        String root = System.getProperty("parity.failures.dir");
        Path base = (root != null && !root.isBlank())
                    ? Path.of(root)
                    : Path.of(System.getProperty("java.io.tmpdir"), "parity-failures-fallback");
        return new ParityFailureBundle(base, ParityScenarioMap.resolve(classDotMethod, byteTier), lane);
    }

    ParityFailureBundle stdout(String s) { this.stdout = s == null ? "" : s; return this; }
    ParityFailureBundle stderr(String s) { this.stderr = s == null ? "" : s; return this; }

    ParityFailureBundle artifactsCompared(String... items)
    {
        for (String i : items)
            artifactsCompared.add(i);
        return this;
    }

    ParityFailureBundle provenance(String cassandraVersion,
                                   String cassandraGitSha,
                                   Path fixturePath,
                                   List<String> componentList,
                                   String commandLine)
    {
        this.cassandraVersion = orDefault(cassandraVersion, "unknown");
        this.cassandraGitSha = orDefault(cassandraGitSha, "0000000");
        this.fixturePath = fixturePath;
        this.componentList = componentList != null ? componentList : new ArrayList<>();
        this.commandLine = orDefault(commandLine, "<unset>");
        return this;
    }

    /**
     * Add the four {@code byte_for_byte} diff artifacts for one compared
     * component: {@code <component>.byte-diff.txt}, {@code <component>.offset-diff.txt},
     * the shared {@code checksums.txt}, and {@code component_inventory.txt}. Each
     * becomes a record {@code diffs[]} entry (byte_diff / offset_diff /
     * checksum_diff / component_inventory).
     */
    ParityFailureBundle byteForByteComponent(String component,
                                             String byteDiffBody,
                                             String offsetDiffBody,
                                             String checksumsBody,
                                             String componentInventoryBody)
    {
        diffFiles.add(new DiffFile("byte_diff", component + ".byte-diff.txt", byteDiffBody));
        diffFiles.add(new DiffFile("offset_diff", component + ".offset-diff.txt", offsetDiffBody));
        // checksums.txt is shared across components; keep a single copy (last wins,
        // and callers pass the full both-engine summary).
        replaceOrAdd(new DiffFile("checksum_diff", "checksums.txt", checksumsBody));
        replaceOrAdd(new DiffFile("component_inventory", "component_inventory.txt",
                                  componentInventoryBody));
        return this;
    }

    /**
     * Add the {@code live_log} diff entry (nightly_docker byte tier, owner
     * decision 5): the failing comparison's captured stdout/stderr, NOT the full
     * container log. Written to {@code diffs/live-log.txt}.
     */
    ParityFailureBundle liveLog(String body)
    {
        diffFiles.add(new DiffFile("live_log", "live-log.txt", body));
        return this;
    }

    /**
     * Add the {@code canonical_semantic} artifacts: the normalized {@code jsonl.diff}
     * (record {@code diffs[]} entry, kind jsonl_diff) plus raw {@code reference.jsonl}
     * and {@code candidate.jsonl} bundle files.
     */
    ParityFailureBundle jsonl(String normalizedDiff, String referenceJsonl, String candidateJsonl)
    {
        diffFiles.add(new DiffFile("jsonl_diff", "jsonl.diff", normalizedDiff));
        rawFiles.add(new RawFile("reference.jsonl", referenceJsonl));
        rawFiles.add(new RawFile("candidate.jsonl", candidateJsonl));
        return this;
    }

    /** Write the whole bundle to disk; returns the bundle dir. */
    Path emit() throws IOException
    {
        Path bundleDir = root.resolve("parity-failures").resolve(tier).resolve(scenarioId);
        // Start clean so a re-run does not mix old and new output.
        deleteRecursively(bundleDir);
        Files.createDirectories(bundleDir);

        Files.writeString(bundleDir.resolve("stdout.txt"), stdout, StandardCharsets.UTF_8);
        Files.writeString(bundleDir.resolve("stderr.txt"), stderr, StandardCharsets.UTF_8);

        Path diffsDir = bundleDir.resolve("diffs");
        Files.createDirectories(diffsDir);
        List<ParityFailureArtifact.Diff> recordDiffs = new ArrayList<>();
        for (DiffFile df : diffFiles)
        {
            Files.writeString(diffsDir.resolve(df.fileName), df.body, StandardCharsets.UTF_8);
            recordDiffs.add(new ParityFailureArtifact.Diff(df.kind, "diffs/" + df.fileName));
        }
        for (RawFile rf : rawFiles)
            Files.writeString(diffsDir.resolve(rf.fileName), rf.body, StandardCharsets.UTF_8);

        String datasetSha = fixturePath != null && Files.isRegularFile(fixturePath)
                            ? ParityFailureArtifact.sha256(fixturePath)
                            : "0".repeat(64);
        String fixtureDisplay = fixturePath != null ? fixturePath.toString() : "<none>";
        writeRepro(bundleDir, fixtureDisplay, datasetSha);

        ParityFailureArtifact record = new ParityFailureArtifact();
        record.scenarioId = scenarioId;
        record.lane = lane;
        record.tier = tier;
        record.evidenceType = evidenceType;
        record.artifactsCompared = artifactsCompared;
        record.provenance.cassandraVersion = cassandraVersion;
        record.provenance.cassandraGitSha = cassandraGitSha;
        record.provenance.datasetSha256 = datasetSha;
        record.provenance.fixturePath = fixtureDisplay;
        record.provenance.componentList = componentList;
        record.provenance.commandLine = commandLine;
        record.diffs.addAll(recordDiffs);
        record.reproBundle = "repro/";
        record.writeTo(bundleDir);

        return bundleDir;
    }

    private void writeRepro(Path bundleDir, String fixtureDisplay, String datasetSha)
        throws IOException
    {
        Path reproDir = bundleDir.resolve("repro");
        Path inputsDir = reproDir.resolve("inputs");
        Files.createDirectories(inputsDir);

        String command = "#!/usr/bin/env bash\nset -euo pipefail\n"
                         + "# Reproduce the failing compaction-parity comparison locally.\n"
                         + commandLine + "\n";
        Files.writeString(reproDir.resolve("command.sh"), command, StandardCharsets.UTF_8);

        String instructions = "# Reproducing this compaction-parity failure\n\n"
            + "1. Build the pinned Cassandra source: "
            + "`bash compaction-parity/scripts/bootstrap-cassandra.sh`.\n\n"
            + "2. Build the cqlite binary: `cargo build --features write-support`.\n\n"
            + "3. Run the exact comparison command below (also in `repro/command.sh`); "
            + "this bundle records fixture paths + SHA256 only, not the dataset.\n\n"
            + "```\n" + commandLine + "\n```\n";
        Files.writeString(reproDir.resolve("INSTRUCTIONS.md"), instructions, StandardCharsets.UTF_8);

        String fixtures = "# fixture_path\tdataset_sha256\n"
                          + fixtureDisplay + "\t" + datasetSha + "\n";
        Files.writeString(inputsDir.resolve("fixtures.txt"), fixtures, StandardCharsets.UTF_8);
    }

    private void replaceOrAdd(DiffFile df)
    {
        for (int i = 0; i < diffFiles.size(); i++)
        {
            if (diffFiles.get(i).fileName.equals(df.fileName))
            {
                diffFiles.set(i, df);
                return;
            }
        }
        diffFiles.add(df);
    }

    private static String orDefault(String s, String def)
    {
        return (s == null || s.isBlank()) ? def : s;
    }

    private static void deleteRecursively(Path p) throws IOException
    {
        if (!Files.exists(p))
            return;
        try (var stream = Files.walk(p))
        {
            stream.sorted(java.util.Comparator.reverseOrder())
                  .forEach(path -> {
                      try { Files.deleteIfExists(path); }
                      catch (IOException ignored) { /* best-effort cleanup */ }
                  });
        }
    }

    // ── Diff/inventory body formatters (mirror the Rust cqlite-core helpers so ──
    // ── the two surfaces produce equivalent bodies). ────────────────────────────

    /** {@code checksums.txt}: SHA-256 per component for both engines. */
    static String checksumsBody(Path referenceDir, Path candidateDir)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("# SHA-256 per output component\n\n");
        sb.append("## cassandra-output\n");
        appendChecksums(sb, referenceDir);
        sb.append("\n## cqlite-output\n");
        appendChecksums(sb, candidateDir);
        return sb.toString();
    }

    private static void appendChecksums(StringBuilder sb, Path dir)
    {
        if (dir == null || !Files.isDirectory(dir))
            return;
        try (var stream = Files.list(dir))
        {
            Map<String, Path> byKind = new TreeMap<>();
            for (Path p : (Iterable<Path>) stream::iterator)
                if (Files.isRegularFile(p))
                    byKind.put(ParityFailureArtifact.componentKind(p.getFileName().toString()), p);
            for (Map.Entry<String, Path> e : byKind.entrySet())
                sb.append(ParityFailureArtifact.sha256(e.getValue()))
                  .append("  ").append(e.getKey()).append('\n');
        }
        catch (IOException ignored)
        {
            // Best-effort checksum listing.
        }
    }

    /** {@code component_inventory.txt}: expected vs actual component set. */
    static String componentInventoryBody(List<String> expected, List<String> actual)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("component inventory (expected vs actual):\n");
        sb.append("  expected: ").append(String.join(", ", expected)).append('\n');
        sb.append("  actual  : ").append(String.join(", ", actual)).append('\n');
        List<String> missing = new ArrayList<>();
        for (String e : expected)
            if (!actual.contains(e))
                missing.add(e);
        List<String> extra = new ArrayList<>();
        for (String a : actual)
            if (!expected.contains(a))
                extra.add(a);
        if (!missing.isEmpty())
            sb.append("  missing from actual: ").append(String.join(", ", missing)).append('\n');
        if (!extra.isEmpty())
            sb.append("  extra in actual: ").append(String.join(", ", extra)).append('\n');
        return sb.toString();
    }
}
