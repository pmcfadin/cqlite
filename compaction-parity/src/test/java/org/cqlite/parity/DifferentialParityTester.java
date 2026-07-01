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

    /**
     * The emitting workflow file recorded as the {@code lane} in the
     * failure-artifact record (issue #1027). The compaction Java/Gradle harness
     * (logical + byte tiers) is driven by {@code compaction-parity.yml}.
     */
    private static final String LANE = "compaction-parity.yml";

    /**
     * The pinned Cassandra version/git-sha the harness builds against, recorded in
     * the failure-artifact {@code provenance} so it is comparable to the manifest
     * {@code cassandra_source} pin. Overridable via {@code -Dparity.cassandra.version}
     * / {@code -Dparity.cassandra.git.sha} (the Gradle build sets these from the
     * pinned source); the defaults match the manifest's compaction scenarios.
     */
    private static String cassandraVersion()
    {
        return System.getProperty("parity.cassandra.version", "5.0.2");
    }

    private static String cassandraGitSha()
    {
        return System.getProperty("parity.cassandra.git.sha",
                                  "f278f6774fc76465c182041e081982105c3e7dbb");
    }

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
        // Single walk for the candidate output. Mirror the reference "exactly one
        // output" assert for the candidate: a writer regression that emits >1
        // SSTable generation is a real divergence and must fail the LOGICAL gate
        // directly, not stay hidden inside the swallowed byte-tier block (where a
        // duplicate-kind would otherwise be suppressed on PRs).
        List<Path> candidateDataList = candidateDataFiles(outputDir);
        assertEquals("expected exactly one cqlite output Data.db (one SSTable generation) under "
                     + outputDir, 1, candidateDataList.size());
        Path candidateData = candidateDataList.get(0);
        Path candOutDir = artifacts.copyComponentsOf(candidateData, "cqlite-output");

        // ── BYTE tier: per-component cmp, NO allowlist. Computed + persisted ALWAYS;
        //    asserted only when the byte tier is enabled (gradle byteParity). ──
        // CRITICAL: the byte computation/persistence (compare() can throw on a
        // duplicate-kind output, plus file I/O) must NEVER red the LOGICAL hard
        // gate. On the per-PR logical path (byte tier not asserting) we swallow any
        // failure into byte-diff.txt best-effort. Only when byteParity IS the
        // asserting tier (nightly/dispatch) does a byte-tier exception surface.
        ComponentByteComparator.Result byteResult;
        try
        {
            byteResult = ComponentByteComparator.compare(cassOutDir, candOutDir);
            artifacts.write("byte-diff.txt", byteResult.render());
            artifacts.writeChecksums(cassOutDir, candOutDir);
        }
        catch (Exception e)
        {
            if (byteTierEnabled())
                throw e; // byteParity is the asserting tier: surface the failure.
            byteResult = null;
            // Visible, greppable signal so a suppressed byte-tier failure on the
            // logical PR path is never silently invisible in the CI log.
            System.err.println("WARN: byte-tier computation suppressed on logical path: " + e);
            writeByteFailureArtifact(artifacts, e);
        }

        // ── LOGICAL comparison: same sstabledump over both outputs ──
        String referenceJson = normalize(sstabledump(referenceData));
        String candidateJson = normalize(sstabledump(candidateData));
        artifacts.write("reference.jsonl", referenceJson);
        artifacts.write("candidate.jsonl", candidateJson);

        System.out.println("[parity] " + scenarioLabel() + " artifacts: " + artifacts.dir());

        // LOGICAL tier is the hard gate.
        if (!referenceJson.equals(candidateJson))
        {
            // Issue #1027: emit the shared, scenario-id-keyed failure bundle
            // (canonical_semantic → jsonl_diff + raw reference/candidate JSONL)
            // BEFORE failing so a red run yields the manifest-joinable record.
            emitLogicalFailureBundle(candidateData, res, referenceJson, candidateJson);
            assertEquals("logical dump mismatch (cassandra reference vs cqlite candidate)",
                         referenceJson, candidateJson);
        }

        // BYTE tier assertion (opt-in, #842 north star). No allowlist: any
        // component divergence is a failure. (byteResult is non-null whenever the
        // byte tier is the asserting tier — a compute failure would have rethrown.)
        if (byteTierEnabled() && byteResult != null && byteResult.hasMismatch())
        {
            StringBuilder msg = new StringBuilder("BYTE tier mismatch (no allowlist) — "
                + "first diff per divergent component:\n");
            for (ComponentByteComparator.ComponentDiff d : byteResult.mismatches())
                msg.append("  ").append(d).append('\n');
            msg.append("Artifacts: ").append(artifacts.dir());
            // Issue #1027: emit the shared, scenario-id-keyed byte_for_byte bundle
            // (byte/offset/checksum/component_inventory + live_log, tier
            // nightly_docker) BEFORE failing.
            emitByteFailureBundle(byteResult, candidateData, res, cassOutDir, candOutDir);
            fail(msg.toString());
        }
    }

    /**
     * Issue #1027 (task 3.2): emit the byte_for_byte failure bundle keyed by the
     * manifest scenario id. Runs on the byte tier (nightly_docker), so it also
     * carries a {@code live_log} diff (the failing comparison's stdout/stderr, per
     * owner decision 5 — NOT the full container log). Bundle emission is
     * best-effort: it must never mask the real {@code fail()} that follows.
     */
    private void emitByteFailureBundle(ComponentByteComparator.Result byteResult,
                                       Path candidateData,
                                       CqliteCompactionRunner.Result res,
                                       Path cassOutDir,
                                       Path candOutDir)
    {
        try
        {
            List<String> refKinds = ParityFailureArtifact.componentKinds(cassOutDir);
            List<String> candKinds = ParityFailureArtifact.componentKinds(candOutDir);
            String checksums = ParityFailureBundle.checksumsBody(cassOutDir, candOutDir);
            String inventory = ParityFailureBundle.componentInventoryBody(refKinds, candKinds);

            ParityFailureBundle bundle =
                ParityFailureBundle.forMethod(scenarioLabel(), true, LANE)
                    .stdout(res.stdout)
                    .stderr(res.stderr)
                    .artifactsCompared("bytes", "offsets", "checksums", "component_files")
                    .provenance(cassandraVersion(), cassandraGitSha(), candidateData,
                                candKinds, res.commandLine())
                    .liveLog(liveLogBody(res));

            // One byte_for_byte diff set per divergent component (the checksum +
            // inventory files are shared and de-duplicated inside the bundle).
            for (ComponentByteComparator.ComponentDiff d : byteResult.mismatches())
                bundle.byteForByteComponent(d.kind, d.byteDiffBody(), d.offsetDiffBody(),
                                            checksums, inventory);
            // No divergent component but the tier still failed (defensive): still
            // attach the checksum + inventory so the bundle is conforming.
            if (byteResult.mismatches().isEmpty())
                bundle.byteForByteComponent("Data.db", byteResult.render(), "no per-component offset\n",
                                            checksums, inventory);

            Path dir = bundle.emit();
            System.out.println("[parity] byte failure bundle: " + dir);
        }
        catch (Exception e)
        {
            System.err.println("WARN: could not emit byte failure bundle (#1027): " + e);
        }
    }

    /**
     * Issue #1027 (task 3.1): emit the canonical_semantic failure bundle
     * (required_parity) with the normalized {@code jsonl.diff} + both raw JSONL
     * sources. Best-effort: never masks the {@code assertEquals} that follows.
     */
    private void emitLogicalFailureBundle(Path candidateData,
                                          CqliteCompactionRunner.Result res,
                                          String referenceJson,
                                          String candidateJson)
    {
        try
        {
            List<String> candKinds = ParityFailureArtifact.componentKinds(candidateData.getParent());
            String jsonlDiff = jsonlDiffBody(referenceJson, candidateJson);
            Path dir = ParityFailureBundle.forMethod(scenarioLabel(), false, LANE)
                .stdout(res.stdout)
                .stderr(res.stderr)
                .artifactsCompared("jsonl")
                .provenance(cassandraVersion(), cassandraGitSha(), candidateData,
                            candKinds, res.commandLine())
                .jsonl(jsonlDiff, referenceJson, candidateJson)
                .emit();
            System.out.println("[parity] logical failure bundle: " + dir);
        }
        catch (Exception e)
        {
            System.err.println("WARN: could not emit logical failure bundle (#1027): " + e);
        }
    }

    /** The live_log body: the failing comparison's captured child output only. */
    private static String liveLogBody(CqliteCompactionRunner.Result res)
    {
        return "# Failing comparison live log (cqlite compact child process)\n"
             + "# (owner decision 5: the failing comparison's stdout/stderr, NOT the full container log)\n\n"
             + "## command\n" + res.commandLine() + "\n\n"
             + "## exit code\n" + res.exitCode + "\n\n"
             + "## stdout\n" + res.stdout + "\n\n"
             + "## stderr\n" + res.stderr + "\n";
    }

    /** First-differing-line normalized JSONL diff body (mirrors the Rust helper). */
    private static String jsonlDiffBody(String referenceJson, String candidateJson)
    {
        String[] ref = referenceJson.split("\n", -1);
        String[] cand = candidateJson.split("\n", -1);
        StringBuilder sb = new StringBuilder();
        sb.append("normalized-JSONL diff (cassandra ").append(ref.length)
          .append(" line(s), cqlite ").append(cand.length).append(" line(s))\n");
        int max = Math.max(ref.length, cand.length);
        for (int i = 0; i < max; i++)
        {
            String e = i < ref.length ? ref[i] : "<missing>";
            String a = i < cand.length ? cand[i] : "<missing>";
            if (!e.equals(a))
            {
                sb.append("first differing line ").append(i).append(":\n");
                sb.append("  cassandra: ").append(e).append('\n');
                sb.append("  cqlite   : ").append(a).append('\n');
                return sb.toString();
            }
        }
        sb.append("no line-level difference detected (length mismatch only)\n");
        return sb.toString();
    }

    /**
     * Record a byte-tier computation failure into {@code byte-diff.txt} best-effort,
     * for the logical (non-asserting) path only. Persisting this is itself
     * best-effort — it must never mask or override the logical result.
     */
    private static void writeByteFailureArtifact(ParityArtifacts artifacts, Exception e)
    {
        try
        {
            java.io.StringWriter sw = new java.io.StringWriter();
            e.printStackTrace(new java.io.PrintWriter(sw));
            artifacts.write("byte-diff.txt",
                "byte-tier computation failed (non-blocking on the logical PR path; the "
                + "byte tier only gates under -Dparity.tier=byte / gradle byteParity):\n\n" + sw);
        }
        catch (IOException ignored)
        {
            // Artifact persistence is best-effort here; do not let it touch the logical gate.
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

    /**
     * Every {@code -Data.db} under {@code outputDir} — one per output SSTable
     * generation. The caller walks once and asserts exactly one (single-output
     * guarantee), avoiding a redundant second walk.
     */
    private static List<Path> candidateDataFiles(Path outputDir) throws IOException
    {
        List<Path> data = new ArrayList<>();
        try (var stream = Files.walk(outputDir))
        {
            stream.filter(p -> p.getFileName().toString().endsWith("-Data.db")).forEach(data::add);
        }
        return data;
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
