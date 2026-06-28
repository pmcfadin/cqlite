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
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Preserves the inputs and outputs of one differential-parity scenario so a CI
 * failure (or success) can be inspected offline (issue #1016, AC3).
 *
 * <p>Writes everything under {@code <root>/<scenario>/}:
 * <ul>
 *   <li>{@code inputs/} — the input SSTables fed to BOTH engines</li>
 *   <li>{@code cassandra-output/} — Cassandra reference output components</li>
 *   <li>{@code cqlite-output/} — cqlite candidate output components</li>
 *   <li>{@code schema.cql} — the standalone DDL handed to cqlite</li>
 *   <li>{@code commands.txt} — exact command lines (cqlite compact, sstabledump)</li>
 *   <li>{@code cqlite-compact.stdout} / {@code cqlite-compact.stderr}</li>
 *   <li>{@code reference.jsonl} / {@code candidate.jsonl} — normalized dumps</li>
 *   <li>{@code checksums.txt} — SHA-256 per component, both engines</li>
 *   <li>{@code byte-diff.txt} — first byte/offset diff per component</li>
 * </ul>
 *
 * <p>The artifacts root comes from {@code -Dparity.artifacts.dir} (set by Gradle);
 * if unset it falls back to a temp dir so local ad-hoc runs still preserve output.
 */
final class ParityArtifacts
{
    private final Path dir;

    private ParityArtifacts(Path dir)
    {
        this.dir = dir;
    }

    /** Create (or reuse) the per-scenario artifacts directory. */
    static ParityArtifacts forScenario(String scenario) throws IOException
    {
        String root = System.getProperty("parity.artifacts.dir");
        Path base = (root != null && !root.isBlank())
                    ? Path.of(root)
                    : Files.createTempDirectory("parity-artifacts");
        Path scenarioDir = base.resolve(sanitize(scenario));
        // Start clean so a re-run does not mix old and new output.
        if (Files.exists(scenarioDir))
            deleteRecursively(scenarioDir);
        Files.createDirectories(scenarioDir);
        return new ParityArtifacts(scenarioDir);
    }

    Path dir()
    {
        return dir;
    }

    /** Copy every regular file from {@code src} into {@code <scenario>/<subdir>/}. */
    Path copyDir(Path src, String subdir) throws IOException
    {
        Path dest = dir.resolve(subdir);
        Files.createDirectories(dest);
        if (Files.isDirectory(src))
        {
            try (var stream = Files.list(src))
            {
                for (Path p : (Iterable<Path>) stream::iterator)
                {
                    if (Files.isRegularFile(p))
                        Files.copy(p, dest.resolve(p.getFileName().toString()),
                                   StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
        return dest;
    }

    /**
     * Copy the components of a single SSTable (all files sharing {@code dataFile}'s
     * prefix up to {@code -Data.db}) from its directory into {@code <scenario>/<subdir>/}.
     */
    Path copyComponentsOf(Path dataFile, String subdir) throws IOException
    {
        Path dest = dir.resolve(subdir);
        Files.createDirectories(dest);
        String dataName = dataFile.getFileName().toString();
        if (!dataName.endsWith("Data.db"))
            throw new IOException("not a Data.db component: " + dataFile);
        String prefix = dataName.substring(0, dataName.length() - "Data.db".length());
        Path srcDir = dataFile.getParent();
        try (var stream = Files.list(srcDir))
        {
            for (Path p : (Iterable<Path>) stream::iterator)
            {
                if (Files.isRegularFile(p) && p.getFileName().toString().startsWith(prefix))
                    Files.copy(p, dest.resolve(p.getFileName().toString()),
                               StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return dest;
    }

    void write(String name, String content) throws IOException
    {
        Files.writeString(dir.resolve(name), content, StandardCharsets.UTF_8);
    }

    /** Write a SHA-256 checksum summary for both engines' components. */
    void writeChecksums(Path referenceDir, Path candidateDir) throws IOException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("# SHA-256 per output component\n\n");
        sb.append("## cassandra-output\n");
        appendChecksums(sb, referenceDir);
        sb.append("\n## cqlite-output\n");
        appendChecksums(sb, candidateDir);
        write("checksums.txt", sb.toString());
    }

    private static void appendChecksums(StringBuilder sb, Path componentDir) throws IOException
    {
        Map<String, Path> byKind = ComponentByteComparator.componentsByKind(componentDir);
        // Deterministic order by component kind.
        for (Map.Entry<String, Path> e : new TreeMap<>(byKind).entrySet())
            sb.append(sha256(e.getValue())).append("  ").append(e.getKey()).append('\n');
    }

    private static String sha256(Path file) throws IOException
    {
        try
        {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(Files.readAllBytes(file));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash)
                hex.append(String.format("%02x", b));
            return hex.toString();
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IOException("SHA-256 unavailable", e);
        }
    }

    private static String sanitize(String s)
    {
        return s.replaceAll("[^A-Za-z0-9_.-]", "_");
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
}
