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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pure-Java unit tests for {@link ComponentByteComparator} (issue #1016).
 *
 * <p>These touch only {@code ComponentByteComparator} + {@code java.nio}, with no
 * Cassandra or cqlite dependency at runtime. NOTE: they cannot be COMPILED without
 * the pinned Cassandra checkout, because they share the {@code src/test/java}
 * source set with {@link DifferentialParityTester} (which imports Cassandra
 * classes), so the whole source set won't compile until
 * {@code scripts/bootstrap-cassandra.sh} has run. In CI they run as fast cases
 * inside the normal {@code gradle test} / {@code gradle byteParity} suite.
 */
public class ComponentByteComparatorTest
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

    private static ComponentByteComparator.ComponentDiff byKind(
        ComponentByteComparator.Result r, String kind)
    {
        for (ComponentByteComparator.ComponentDiff d : r.components)
            if (d.kind.equals(kind))
                return d;
        fail("no component diff for kind " + kind);
        return null; // unreachable
    }

    // (a) all-equal files → no diff.
    @Test
    public void allEqualComponentsHaveNoMismatch() throws IOException
    {
        Path ref = tmp.newFolder("ref-a").toPath();
        Path cand = tmp.newFolder("cand-a").toPath();
        writeComponent(ref, "nb-1-big-Data.db", 1, 2, 3, 4);
        writeComponent(cand, "da-7-bti-Data.db", 1, 2, 3, 4); // different prefix, same kind
        writeComponent(ref, "nb-1-big-TOC.txt", 9, 9);
        writeComponent(cand, "da-7-bti-TOC.txt", 9, 9);

        ComponentByteComparator.Result r = ComponentByteComparator.compare(ref, cand);
        assertFalse("no component should differ", r.hasMismatch());
        assertEquals(ComponentByteComparator.Status.EQUAL, byKind(r, "Data.db").status);
        assertEquals(ComponentByteComparator.Status.EQUAL, byKind(r, "TOC.txt").status);
    }

    // (b) same-length, one differing byte → correct first-diff offset + both values.
    @Test
    public void sameLengthDifferenceReportsFirstOffsetAndBytes() throws IOException
    {
        Path ref = tmp.newFolder("ref-b").toPath();
        Path cand = tmp.newFolder("cand-b").toPath();
        writeComponent(ref, "nb-1-big-Data.db", 0x10, 0x11, 0x12, 0x13);
        writeComponent(cand, "nb-2-big-Data.db", 0x10, 0x11, 0xAB, 0x13);

        ComponentByteComparator.Result r = ComponentByteComparator.compare(ref, cand);
        assertTrue(r.hasMismatch());
        ComponentByteComparator.ComponentDiff d = byKind(r, "Data.db");
        assertEquals(ComponentByteComparator.Status.DIFFER, d.status);
        assertEquals(2L, d.offset);
        assertEquals(0x12, d.refByte);
        assertEquals(0xAB, d.candByte);
        assertEquals(4L, d.refLen);
        assertEquals(4L, d.candLen);
        assertTrue(d.toString().contains("0x12"));
        assertTrue(d.toString().contains("0xAB"));
    }

    // (c) equal prefix, candidate shorter → divergence at truncation, EOF sentinel.
    @Test
    public void truncationRendersEofNotFf() throws IOException
    {
        Path ref = tmp.newFolder("ref-c").toPath();
        Path cand = tmp.newFolder("cand-c").toPath();
        writeComponent(ref, "nb-1-big-Index.db", 0x0A, 0x0B, 0x0C, 0x0D, 0x0E);
        writeComponent(cand, "nb-1-big-Index.db", 0x0A, 0x0B, 0x0C); // shorter

        ComponentByteComparator.Result r = ComponentByteComparator.compare(ref, cand);
        ComponentByteComparator.ComponentDiff d = byKind(r, "Index.db");
        assertEquals(ComponentByteComparator.Status.DIFFER, d.status);
        assertEquals(3L, d.offset);
        assertEquals(0x0D, d.refByte);
        assertEquals("absent byte must use the -1 sentinel", -1, d.candByte);
        assertEquals(5L, d.refLen);
        assertEquals(3L, d.candLen);
        String rendered = d.toString();
        assertTrue("absent byte must render as EOF: " + rendered, rendered.contains("EOF"));
        assertTrue("present byte still hex: " + rendered, rendered.contains("0x0D"));
        assertFalse("EOF must not render as 0xFF: " + rendered, rendered.contains("0xFF"));
    }

    // (d) reference-only component.
    @Test
    public void referenceOnlyComponentIsMismatch() throws IOException
    {
        Path ref = tmp.newFolder("ref-d").toPath();
        Path cand = tmp.newFolder("cand-d").toPath();
        writeComponent(ref, "nb-1-big-Data.db", 1, 2);
        writeComponent(cand, "nb-1-big-Data.db", 1, 2);
        writeComponent(ref, "nb-1-big-Filter.db", 7); // only on reference side

        ComponentByteComparator.Result r = ComponentByteComparator.compare(ref, cand);
        assertTrue(r.hasMismatch());
        assertEquals(ComponentByteComparator.Status.REFERENCE_ONLY, byKind(r, "Filter.db").status);
        assertEquals(ComponentByteComparator.Status.EQUAL, byKind(r, "Data.db").status);
    }

    // (e) candidate-only component.
    @Test
    public void candidateOnlyComponentIsMismatch() throws IOException
    {
        Path ref = tmp.newFolder("ref-e").toPath();
        Path cand = tmp.newFolder("cand-e").toPath();
        writeComponent(ref, "nb-1-big-Data.db", 1, 2);
        writeComponent(cand, "nb-1-big-Data.db", 1, 2);
        writeComponent(cand, "nb-1-big-CompressionInfo.db", 5); // only on candidate side

        ComponentByteComparator.Result r = ComponentByteComparator.compare(ref, cand);
        assertTrue(r.hasMismatch());
        assertEquals(ComponentByteComparator.Status.CANDIDATE_ONLY,
                     byKind(r, "CompressionInfo.db").status);
    }

    // (f) duplicate component kind (>1 generation) → fail loud.
    @Test
    public void duplicateComponentKindFailsLoud() throws IOException
    {
        Path ref = tmp.newFolder("ref-f").toPath();
        Path cand = tmp.newFolder("cand-f").toPath();
        writeComponent(ref, "nb-1-big-Data.db", 1, 2);
        writeComponent(ref, "nb-2-big-Data.db", 3, 4); // second generation → same kind
        writeComponent(cand, "nb-1-big-Data.db", 1, 2);

        try
        {
            ComponentByteComparator.compare(ref, cand);
            fail("duplicate component kind must throw, not silently drop a file");
        }
        catch (IOException expected)
        {
            assertNotNull(expected.getMessage());
            assertTrue("message should name the duplicate kind: " + expected.getMessage(),
                       expected.getMessage().contains("duplicate component kind"));
        }
    }
}
