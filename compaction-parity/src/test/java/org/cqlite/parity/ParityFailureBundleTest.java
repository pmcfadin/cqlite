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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pure-Java tests for the shared, scenario-id-keyed failure bundle emitted by the
 * compaction harness (issue #1027, task 3.1/3.2).
 *
 * <p>These touch only {@link ParityFailureBundle} / {@link ParityFailureArtifact}
 * / {@link ComponentByteComparator} + {@code java.nio} — NO Cassandra or cqlite
 * runtime dependency. (Like {@link ComponentByteComparatorTest} they cannot be
 * COMPILED without the pinned Cassandra checkout because they share the
 * {@code src/test/java} source set with {@link DifferentialParityTester}.)
 *
 * <p>The forced-mismatch tests exercise the emitter path the byte tier
 * ({@code gradle byteParity}, nightly_docker) and logical tier ({@code gradle
 * test}, required_parity) call on a real failure, and assert the produced
 * {@code failure-artifact.json} conforms to
 * {@code test-data/parity-failure-artifact.schema.json}.
 */
public class ParityFailureBundleTest
{
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static void writeComponent(Path dir, String name, int... bytes) throws IOException
    {
        byte[] b = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++)
            b[i] = (byte) bytes[i];
        Files.write(dir.resolve(name), b);
    }

    /**
     * A FORCED byte mismatch produces a conforming byte_for_byte bundle keyed by
     * the manifest scenario id, tier nightly_docker, with all four byte diff kinds
     * + a live_log entry and a valid failure-artifact.json (task 3.1 + 3.2).
     */
    @Test
    public void forcedByteMismatchEmitsConformingScenarioIdKeyedBundle() throws IOException
    {
        Path root = tmp.newFolder("failures-root").toPath();
        System.setProperty("parity.failures.dir", root.toString());
        try
        {
            // Two output dirs that differ in Data.db (forced mismatch).
            Path cassOut = tmp.newFolder("cass-out").toPath();
            Path candOut = tmp.newFolder("cand-out").toPath();
            writeComponent(cassOut, "nb-1-big-Data.db", 0x10, 0x11, 0x12, 0x13);
            writeComponent(candOut, "da-7-bti-Data.db", 0x10, 0x11, 0xAB, 0x13);
            writeComponent(cassOut, "nb-1-big-TOC.txt", 9, 9);
            writeComponent(candOut, "da-7-bti-TOC.txt", 9, 9);

            ComponentByteComparator.Result byteResult =
                ComponentByteComparator.compare(cassOut, candOut);
            assertTrue("fixture must force a mismatch", byteResult.hasMismatch());

            Path fixture = candOut.resolve("da-7-bti-Data.db");
            List<String> refKinds = ParityFailureArtifact.componentKinds(cassOut);
            List<String> candKinds = ParityFailureArtifact.componentKinds(candOut);
            String checksums = ParityFailureBundle.checksumsBody(cassOut, candOut);
            String inventory = ParityFailureBundle.componentInventoryBody(refKinds, candKinds);

            ParityFailureBundle bundle =
                ParityFailureBundle.forMethod(
                        "BasicDifferentialTest.liveRowsLastWriteWinsAcrossTwoSSTables",
                        true, "compaction-parity.yml")
                    .stdout("compact stdout")
                    .stderr("compact stderr")
                    .artifactsCompared("bytes", "offsets", "checksums", "component_files")
                    .provenance("5.0.2", "f278f6774fc76465c182041e081982105c3e7dbb",
                                fixture, candKinds,
                                "cd compaction-parity && gradle --no-daemon byteParity")
                    .liveLog("failing comparison stdout/stderr");
            for (ComponentByteComparator.ComponentDiff d : byteResult.mismatches())
                bundle.byteForByteComponent(d.kind, d.byteDiffBody(), d.offsetDiffBody(),
                                            checksums, inventory);

            Path bundleDir = bundle.emit();

            // Keyed by scenario id under the nightly_docker tier.
            assertEquals("bundle dir must be the manifest scenario id",
                         ParityScenarioMap.BYTE_SCENARIO,
                         bundleDir.getFileName().toString());
            assertEquals("byte tier is nightly_docker",
                         "nightly_docker", bundleDir.getParent().getFileName().toString());
            assertEquals("parity-failures",
                         bundleDir.getParent().getParent().getFileName().toString());

            // Required bundle files.
            assertTrue(Files.isRegularFile(bundleDir.resolve("failure-artifact.json")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("stdout.txt")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("stderr.txt")));
            assertTrue(Files.isDirectory(bundleDir.resolve("diffs")));
            assertTrue(Files.isDirectory(bundleDir.resolve("repro")));

            // All four byte diff kinds + live_log are present, with matching files.
            assertTrue(Files.isRegularFile(bundleDir.resolve("diffs/Data.db.byte-diff.txt")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("diffs/Data.db.offset-diff.txt")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("diffs/checksums.txt")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("diffs/component_inventory.txt")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("diffs/live-log.txt")));

            // Repro bundle.
            assertTrue(Files.isRegularFile(bundleDir.resolve("repro/command.sh")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("repro/INSTRUCTIONS.md")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("repro/inputs/fixtures.txt")));

            String json = Files.readString(bundleDir.resolve("failure-artifact.json"));
            assertConformingRecord(json, bundleDir, "nightly_docker", "byte_for_byte",
                                   ParityScenarioMap.BYTE_SCENARIO);
            // The four byte kinds + live_log must all appear as diffs[].kind.
            for (String kind : new String[]{"byte_diff", "offset_diff", "checksum_diff",
                                            "component_inventory", "live_log"})
                assertTrue("record diffs[] must include kind " + kind,
                           json.contains("\"kind\": \"" + kind + "\""));
        }
        finally
        {
            System.clearProperty("parity.failures.dir");
        }
    }

    /**
     * A FORCED logical (canonical_semantic) mismatch produces a conforming bundle
     * keyed by the logical manifest scenario id, tier required_parity, with a
     * jsonl_diff + raw reference/candidate JSONL (task 3.1).
     */
    @Test
    public void forcedLogicalMismatchEmitsCanonicalSemanticBundle() throws IOException
    {
        Path root = tmp.newFolder("failures-root-logical").toPath();
        System.setProperty("parity.failures.dir", root.toString());
        try
        {
            Path fixture = tmp.newFolder("fixture-dir").toPath().resolve("nb-1-big-Data.db");
            Files.write(fixture, new byte[]{1, 2, 3, 4});

            Path bundleDir = ParityFailureBundle.forMethod(
                        "BasicDifferentialTest.liveRowsLastWriteWinsAcrossTwoSSTables",
                        false, "compaction-parity.yml")
                .stdout("").stderr("")
                .artifactsCompared("jsonl")
                .provenance("5.0.2", "f278f6774fc76465c182041e081982105c3e7dbb",
                            fixture, List.of("Data.db"),
                            "cd compaction-parity && gradle --no-daemon test")
                .jsonl("first differing line 0\n", "{\"a\":1}\n", "{\"a\":2}\n")
                .emit();

            assertEquals(ParityScenarioMap.LOGICAL_SCENARIO,
                         bundleDir.getFileName().toString());
            assertEquals("required_parity", bundleDir.getParent().getFileName().toString());
            assertTrue(Files.isRegularFile(bundleDir.resolve("diffs/jsonl.diff")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("diffs/reference.jsonl")));
            assertTrue(Files.isRegularFile(bundleDir.resolve("diffs/candidate.jsonl")));

            String json = Files.readString(bundleDir.resolve("failure-artifact.json"));
            assertConformingRecord(json, bundleDir, "required_parity", "canonical_semantic",
                                   ParityScenarioMap.LOGICAL_SCENARIO);
            assertTrue(json.contains("\"kind\": \"jsonl_diff\""));
        }
        finally
        {
            System.clearProperty("parity.failures.dir");
        }
    }

    /** The scenario_id map returns valid manifest ids + matching tier/evidence. */
    @Test
    public void scenarioMapKeysByManifestId()
    {
        ParityScenarioMap.Resolution byteRes = ParityScenarioMap.resolve(
            "BasicDifferentialTest.liveRowsLastWriteWinsNoClustering", true);
        assertEquals(ParityScenarioMap.BYTE_SCENARIO, byteRes.scenarioId);
        assertEquals("nightly_docker", byteRes.tier);
        assertEquals("byte_for_byte", byteRes.evidenceType);
        assertTrue(byteRes.scenarioId.startsWith("cass."));

        ParityScenarioMap.Resolution logicalRes = ParityScenarioMap.resolve(
            "BasicDifferentialTest.liveRowsLastWriteWinsNoClustering", false);
        assertEquals(ParityScenarioMap.LOGICAL_SCENARIO, logicalRes.scenarioId);
        assertEquals("required_parity", logicalRes.tier);
        assertEquals("canonical_semantic", logicalRes.evidenceType);
    }

    /**
     * Assert a written failure-artifact.json conforms to
     * {@code test-data/parity-failure-artifact.schema.json}: all required
     * top-level + provenance fields present; tier/evidence are the expected closed
     * enum values; and every diffs[].path + repro_bundle pointer resolves inside
     * the bundle.
     */
    private static void assertConformingRecord(String json, Path bundleDir,
                                               String expectTier, String expectEvidence,
                                               String expectScenario) throws IOException
    {
        // schema_version const 1
        assertTrue("schema_version must be 1", json.contains("\"schema_version\": 1"));
        // required top-level fields
        for (String field : new String[]{"scenario_id", "lane", "tier", "evidence_type",
                                         "artifacts_compared", "provenance", "diffs",
                                         "repro_bundle"})
            assertTrue("record must contain top-level field " + field,
                       json.contains("\"" + field + "\""));
        // required provenance fields
        for (String field : new String[]{"cassandra_version", "cassandra_git_sha",
                                         "dataset_sha256", "fixture_path", "component_list",
                                         "command_line", "stdout", "stderr"})
            assertTrue("provenance must contain field " + field,
                       json.contains("\"" + field + "\""));
        // tier / evidence_type / scenario_id values
        assertTrue(json.contains("\"tier\": \"" + expectTier + "\""));
        assertTrue(json.contains("\"evidence_type\": \"" + expectEvidence + "\""));
        assertTrue(json.contains("\"scenario_id\": \"" + expectScenario + "\""));
        // dataset_sha256 must be 64 lowercase hex (schema pattern)
        assertTrue("dataset_sha256 must be 64 hex chars",
                   json.matches("(?s).*\"dataset_sha256\": \"[0-9a-f]{64}\".*"));
        // repro_bundle resolves
        assertTrue("repro/ pointer must resolve inside the bundle",
                   Files.isDirectory(bundleDir.resolve("repro")));
        // Every diffs[].path resolves inside the bundle.
        java.util.regex.Matcher m =
            java.util.regex.Pattern.compile("\"path\": \"([^\"]+)\"").matcher(json);
        boolean sawPath = false;
        while (m.find())
        {
            sawPath = true;
            Path p = bundleDir.resolve(m.group(1));
            assertTrue("diffs[].path must resolve inside the bundle: " + m.group(1),
                       Files.isRegularFile(p));
        }
        assertTrue("record must have at least one diffs[] pointer", sawPath);
        assertFalse("json must not be empty", json.isBlank());
    }
}
