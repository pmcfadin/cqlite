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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Byte tier of the compaction-parity comparison (issue #1016, #842 north star).
 *
 * <p>Compares EVERY output component file produced by the two compaction engines
 * byte-for-byte with NO allowlist — Data.db, Index.db, Statistics.db, Summary.db,
 * Filter.db, CompressionInfo.db, Digest.crc32, TOC.txt (and any other components,
 * e.g. BTI Partitions.db/Rows.db) as present. For each component it reports the
 * FIRST differing byte offset, and flags components present on only one side.
 *
 * <p>The component "kind" is the filename token after the last {@code -}, so it is
 * stable across the {@code nb}/{@code oa}/{@code da} generation+format prefixes
 * (e.g. {@code nb-1-big-Data.db} → {@code Data.db}). This lets us match a
 * Cassandra output component to the corresponding cqlite output component even
 * though their generation strings differ.
 */
final class ComponentByteComparator
{
    enum Status { EQUAL, DIFFER, REFERENCE_ONLY, CANDIDATE_ONLY }

    /** One component's comparison outcome. */
    static final class ComponentDiff
    {
        final String kind;
        final Status status;
        final long offset;     // first differing offset, or -1 when not applicable
        final int refByte;     // unsigned byte at offset on the reference side, or -1
        final int candByte;    // unsigned byte at offset on the candidate side, or -1
        final long refLen;     // reference component length, or -1 if absent
        final long candLen;    // candidate component length, or -1 if absent

        ComponentDiff(String kind, Status status, long offset,
                      int refByte, int candByte, long refLen, long candLen)
        {
            this.kind = kind;
            this.status = status;
            this.offset = offset;
            this.refByte = refByte;
            this.candByte = candByte;
            this.refLen = refLen;
            this.candLen = candLen;
        }

        boolean isMismatch()
        {
            return status != Status.EQUAL;
        }

        @Override
        public String toString()
        {
            switch (status)
            {
                case EQUAL:
                    return String.format("%-20s EQUAL (%d bytes)", kind, refLen);
                case REFERENCE_ONLY:
                    return String.format("%-20s MISMATCH: present in Cassandra output only (%d bytes)",
                                         kind, refLen);
                case CANDIDATE_ONLY:
                    return String.format("%-20s MISMATCH: present in cqlite output only (%d bytes)",
                                         kind, candLen);
                case DIFFER:
                default:
                    return String.format(
                        "%-20s MISMATCH at offset %d: cassandra=%s cqlite=%s "
                        + "(lengths cassandra=%d cqlite=%d)",
                        kind, offset, hexOrEof(refByte), hexOrEof(candByte), refLen, candLen);
            }
        }

        /**
         * Render a byte as hex, or {@code EOF} when it is the absent-byte sentinel
         * (-1) used in the equal-prefix / different-length case — so a missing byte
         * never prints as a real-looking {@code 0xFF}.
         */
        private static String hexOrEof(int b)
        {
            return b < 0 ? "EOF" : String.format("0x%02X", b & 0xFF);
        }
    }

    /** Aggregate result over all components in the two output directories. */
    static final class Result
    {
        final List<ComponentDiff> components;

        Result(List<ComponentDiff> components)
        {
            this.components = components;
        }

        boolean hasMismatch()
        {
            return components.stream().anyMatch(ComponentDiff::isMismatch);
        }

        List<ComponentDiff> mismatches()
        {
            List<ComponentDiff> m = new ArrayList<>();
            for (ComponentDiff d : components)
                if (d.isMismatch())
                    m.add(d);
            return m;
        }

        /** Human-readable, one component per line; mismatches first. */
        String render()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("# Byte-for-byte component comparison (no allowlist)\n");
            sb.append("# reference = Apache Cassandra compaction output\n");
            sb.append("# candidate = cqlite compact output\n\n");
            if (!hasMismatch())
                sb.append("ALL COMPONENTS BYTE-IDENTICAL\n\n");
            for (ComponentDiff d : components)
                sb.append(d).append('\n');
            return sb.toString();
        }
    }

    private ComponentByteComparator() {}

    /**
     * Compare every component in {@code referenceDir} against the same component
     * (matched by kind) in {@code candidateDir}. Reports the first differing
     * offset per component and flags one-sided components. NO allowlist.
     */
    static Result compare(Path referenceDir, Path candidateDir) throws IOException
    {
        Map<String, Path> ref = componentsByKind(referenceDir);
        Map<String, Path> cand = componentsByKind(candidateDir);

        // Union of kinds, deterministic order.
        TreeMap<String, Boolean> kinds = new TreeMap<>();
        for (String k : ref.keySet())
            kinds.put(k, Boolean.TRUE);
        for (String k : cand.keySet())
            kinds.put(k, Boolean.TRUE);

        List<ComponentDiff> diffs = new ArrayList<>();
        for (String kind : kinds.keySet())
        {
            Path r = ref.get(kind);
            Path c = cand.get(kind);
            if (r != null && c == null)
            {
                diffs.add(new ComponentDiff(kind, Status.REFERENCE_ONLY, -1, -1, -1,
                                            Files.size(r), -1));
            }
            else if (r == null && c != null)
            {
                diffs.add(new ComponentDiff(kind, Status.CANDIDATE_ONLY, -1, -1, -1,
                                            -1, Files.size(c)));
            }
            else if (r != null)
            {
                diffs.add(diffBytes(kind, r, c));
            }
        }
        return new Result(diffs);
    }

    /** Map component kind (token after the last '-') → file path. */
    static Map<String, Path> componentsByKind(Path dir) throws IOException
    {
        Map<String, Path> map = new TreeMap<>();
        if (!Files.isDirectory(dir))
            return map;
        try (var stream = Files.list(dir))
        {
            for (Path p : (Iterable<Path>) stream::iterator)
            {
                if (!Files.isRegularFile(p))
                    continue;
                String name = p.getFileName().toString();
                int dash = name.lastIndexOf('-');
                String kind = dash >= 0 ? name.substring(dash + 1) : name;
                Path prev = map.put(kind, p);
                if (prev != null)
                    // Two files map to the same component kind => more than one SSTable
                    // generation is present. The byte tier compares a single output per
                    // engine, so silently keeping only the last file would violate the
                    // no-allowlist "compare every component" guarantee. Fail loudly.
                    throw new IOException(
                        "duplicate component kind '" + kind + "' in " + dir + " ("
                        + prev.getFileName() + " and " + p.getFileName()
                        + "): more than one SSTable generation present; the byte tier "
                        + "expects exactly one output SSTable per engine");
            }
        }
        return map;
    }

    private static ComponentDiff diffBytes(String kind, Path r, Path c) throws IOException
    {
        byte[] rb = Files.readAllBytes(r);
        byte[] cb = Files.readAllBytes(c);
        int min = Math.min(rb.length, cb.length);
        for (int i = 0; i < min; i++)
        {
            if (rb[i] != cb[i])
                return new ComponentDiff(kind, Status.DIFFER, i, rb[i], cb[i],
                                         rb.length, cb.length);
        }
        if (rb.length != cb.length)
        {
            // Common prefix equal but lengths differ: first divergence is at the
            // truncation point. The byte beyond the shorter file is reported as -1.
            int rByte = rb.length > min ? (rb[min] & 0xFF) : -1;
            int cByte = cb.length > min ? (cb[min] & 0xFF) : -1;
            return new ComponentDiff(kind, Status.DIFFER, min, rByte, cByte,
                                     rb.length, cb.length);
        }
        return new ComponentDiff(kind, Status.EQUAL, -1, -1, -1, rb.length, cb.length);
    }
}
