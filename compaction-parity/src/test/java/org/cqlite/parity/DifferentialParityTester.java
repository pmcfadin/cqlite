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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Rule;
import org.junit.rules.TestName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Differential compaction-parity harness (issue #842).
 *
 * <p>Runs the SAME input SSTables through two compaction engines and asserts the
 * outputs are equivalent:
 *
 * <ol>
 *   <li>REFERENCE — Apache Cassandra's compaction, in-JVM via {@link CQLTester}.</li>
 *   <li>CANDIDATE — the external {@code cqlite compact} binary
 *       ({@link CqliteCompactionRunner}).</li>
 * </ol>
 *
 * <p>This base runs TWO tiers (issue #1016):
 *
 * <ul>
 *   <li><b>LOGICAL</b> (always asserted — hard gate): a canonical
 *       {@code sstabledump} of every output must match. The same
 *       {@code sstabledump} tool is run over both engines' output so the
 *       comparison is apples-to-apples.</li>
 *   <li><b>BYTE</b> (#842 north star): every output component file is compared
 *       byte-for-byte with NO allowlist ({@link ComponentByteComparator}),
 *       reporting the first byte/offset diff per component. The diff is ALWAYS
 *       computed and written to the preserved artifacts; it is ASSERTED only when
 *       the byte tier is enabled via {@code -Dparity.tier=byte} (the
 *       {@code byteParity} Gradle task), so {@code gradle test} stays a pure
 *       logical gate while {@code gradle byteParity} adds the byte assertion.</li>
 * </ul>
 *
 * <p>Every run preserves artifacts (inputs, both outputs, schema, exact command
 * lines, stdout/stderr, normalized JSONL, checksum summary, byte diff) under
 * {@code -Dparity.artifacts.dir} on success AND failure ({@link ParityArtifacts}).
 *
 * <p>Determinism: inputs are written with explicit {@code USING TIMESTAMP}; the
 * same {@code gcBefore} is passed to both engines. NOTE: cqlite does not yet purge
 * (issues #845/#848), so scenarios here must not place purgeable tombstones — live
 * data and retained (non-purgeable) tombstones only.
 */
public abstract class DifferentialParityTester extends CQLTester
{
    private static final Pattern EXPIRED_FLAG =
        Pattern.compile("\"expired\"\\s*:\\s*(true|false)");

    @Rule
    public final TestName testName = new TestName();

    private final CqliteCompactionRunner cqlite = CqliteCompactionRunner.fromConfig();

    /** True when the byte tier should be asserted (not just computed + reported). */
    private static boolean byteTierEnabled()
    {
        String tier = System.getProperty("parity.tier", "");
        return tier.equalsIgnoreCase("byte") || Boolean.getBoolean("parity.byte");
    }

    private String scenarioLabel()
    {
        String method = testName.getMethodName();
        return getClass().getSimpleName() + "." + (method == null ? "scenario" : method);
    }

    /**
     * Build inputs, compact with both engines over the same files, and assert the
     * logical dumps match.
     *
     * @param createTableDdl a {@code CREATE TABLE %s (...)} statement (CQLTester
     *                       substitutes the generated table name for {@code %s})
     * @param insertGroups   each inner list is the inserts for one input SSTable,
     *                       flushed independently; provide >= 2 groups so a real
     *                       merge happens
     */
    protected void assertCqliteMatchesCassandra(String createTableDdl,
                                                List<List<String>> insertGroups) throws Exception
    {
        assertTrue("need >= 2 input SSTables for a meaningful merge", insertGroups.size() >= 2);

        String table = createTable(createTableDdl);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (List<String> group : insertGroups)
        {
            for (String insert : group)
                execute(insert);
            flush();
        }

        List<SSTableReader> inputs = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals("each insert group should flush to its own SSTable",
                     insertGroups.size(), inputs.size());

        // Preserve everything for this scenario, on success AND failure.
        ParityArtifacts artifacts = ParityArtifacts.forScenario(scenarioLabel());

        // ── Snapshot the input SSTables for cqlite BEFORE compaction obsoletes them ──
        Path inputDir = Files.createTempDirectory("parity-inputs");
        for (SSTableReader r : inputs)
            copyAllComponents(r.descriptor, inputDir);
        artifacts.copyDir(inputDir, "inputs");

        // cqlite schema file: the same DDL, but fully qualified and standalone.
        Path schemaFile = Files.createTempFile("parity-schema", ".cql");
        String standaloneDdl = createTableDdl.replace("%s", keyspace() + "." + table) + ";";
        Files.writeString(schemaFile, standaloneDdl);
        artifacts.write("schema.cql", standaloneDdl + "\n");

        // No purging for these scenarios: gcBefore well before any deletion time.
        long gcBefore = FBUtilities.nowInSeconds();

        // ── REFERENCE: Cassandra major compaction → a single SSTable ──
        CompactionManager.instance.performMaximal(cfs, false);
        List<SSTableReader> outputs = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals("expected exactly one reference output SSTable", 1, outputs.size());
        Path referenceData = outputs.get(0).descriptor.fileFor(Components.DATA).toPath();
        Path cassOutDir = artifacts.copyComponentsOf(referenceData, "cassandra-output");

        // ── CANDIDATE: cqlite compaction over the SAME inputs ──
        Path outputDir = Files.createTempDirectory("parity-cqlite-out");
        CqliteCompactionRunner.Result res =
            cqlite.compact(inputDir, outputDir, schemaFile, gcBefore, null, 1);

        // Capture the exact command lines + child output BEFORE asserting success,
        // so a failed compaction still leaves a full forensic trail.
        String sstabledumpTool = System.getProperty("cassandra.sstabledump", "<unset>");
        artifacts.write("commands.txt",
            "# cqlite compact\n" + res.commandLine() + "\n\n"
            + "# sstabledump (reference)\n" + sstabledumpTool + " -l " + referenceData + "\n\n"
            + "# sstabledump (candidate)\n" + sstabledumpTool + " -l <cqlite Data.db>\n");
        artifacts.write("cqlite-compact.stdout", res.stdout);
        artifacts.write("cqlite-compact.stderr", res.stderr);

        assertTrue("cqlite compact failed (exit " + res.exitCode + "):\n" + res.stderr + res.stdout,
                   res.succeeded());
        Path candidateData = findSingleData(outputDir);
        Path candOutDir = artifacts.copyComponentsOf(candidateData, "cqlite-output");

        // ── BYTE tier: per-component cmp, NO allowlist. Computed + persisted ALWAYS;
        //    asserted only when the byte tier is enabled (gradle byteParity). ──
        ComponentByteComparator.Result byteResult =
            ComponentByteComparator.compare(cassOutDir, candOutDir);
        artifacts.write("byte-diff.txt", byteResult.render());
        artifacts.writeChecksums(cassOutDir, candOutDir);

        // ── LOGICAL comparison: same sstabledump over both outputs ──
        String referenceJson = normalize(sstabledump(referenceData));
        String candidateJson = normalize(sstabledump(candidateData));
        artifacts.write("reference.jsonl", referenceJson);
        artifacts.write("candidate.jsonl", candidateJson);

        System.out.println("[parity] " + scenarioLabel() + " artifacts: " + artifacts.dir());

        // LOGICAL tier is the hard gate.
        assertEquals("logical dump mismatch (cassandra reference vs cqlite candidate)",
                     referenceJson, candidateJson);

        // BYTE tier assertion (opt-in, #842 north star). No allowlist: any
        // component divergence is a failure.
        if (byteTierEnabled() && byteResult.hasMismatch())
        {
            StringBuilder msg = new StringBuilder("BYTE tier mismatch (no allowlist) — "
                + "first diff per divergent component:\n");
            for (ComponentByteComparator.ComponentDiff d : byteResult.mismatches())
                msg.append("  ").append(d).append('\n');
            msg.append("Artifacts: ").append(artifacts.dir());
            fail(msg.toString());
        }
    }

    /** Copy every on-disk file of an SSTable (all components incl. TOC.txt) into {@code dest}. */
    private static void copyAllComponents(Descriptor descriptor, Path dest) throws IOException
    {
        Path dataFile = descriptor.fileFor(Components.DATA).toPath();
        String dataName = dataFile.getFileName().toString();
        String prefix = dataName.substring(0, dataName.length() - "Data.db".length());
        Path dir = dataFile.getParent();
        try (var stream = Files.list(dir))
        {
            for (Path p : (Iterable<Path>) stream::iterator)
            {
                if (p.getFileName().toString().startsWith(prefix))
                    Files.copy(p, dest.resolve(p.getFileName().toString()));
            }
        }
    }

    private static Path findSingleData(Path outputDir) throws IOException
    {
        List<Path> data = new ArrayList<>();
        try (var stream = Files.walk(outputDir))
        {
            stream.filter(p -> p.getFileName().toString().endsWith("-Data.db")).forEach(data::add);
        }
        assertEquals("expected exactly one cqlite output Data.db under " + outputDir, 1, data.size());
        return data.get(0);
    }

    /** Run Cassandra's {@code sstabledump -l} over a Data.db and return the JSONL. */
    private static String sstabledump(Path dataFile) throws IOException, InterruptedException
    {
        String tool = System.getProperty("cassandra.sstabledump");
        if (tool == null)
            throw new IllegalStateException("-Dcassandra.sstabledump not set");

        Path out = Files.createTempFile("sstabledump", ".jsonl");
        Path err = Files.createTempFile("sstabledump", ".err");
        try
        {
            ProcessBuilder pb = new ProcessBuilder(tool, "-l", dataFile.toString())
                                .redirectOutput(out.toFile())
                                .redirectError(err.toFile());
            // Run the tool on the SAME JDK as this test (Cassandra 5.0 needs 11/17, not 21).
            pb.environment().put("JAVA_HOME", System.getProperty("java.home"));
            Process proc = pb.start();
            if (!proc.waitFor(5, TimeUnit.MINUTES))
            {
                proc.destroyForcibly();
                throw new IOException("sstabledump timed out for " + dataFile);
            }
            if (proc.exitValue() != 0)
                fail("sstabledump failed (exit " + proc.exitValue() + ") for " + dataFile + ":\n"
                     + Files.readString(err));
            return Files.readString(out, StandardCharsets.UTF_8);
        }
        finally
        {
            Files.deleteIfExists(out);
            Files.deleteIfExists(err);
        }
    }

    /**
     * Strip wall-clock-derived noise so byte-equal data renders equal. The
     * {@code expired} flag is computed from the current time at dump (not the data),
     * so two dumps taken seconds apart can differ; {@code expires_at} is still
     * compared. (Only relevant once TTL scenarios are added.)
     */
    private static String normalize(String json)
    {
        return EXPIRED_FLAG.matcher(json).replaceAll("\"expired\" : <normalized>");
    }
}
