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
import java.util.concurrent.TimeUnit;

/**
 * Drives the external {@code cqlite compact} CLI command — the candidate side of
 * the differential compaction-parity comparison (issue #842).
 *
 * <p>This class has no Cassandra dependencies on purpose: it is the seam between
 * the JVM harness (which builds inputs + runs the reference compaction) and the
 * Rust binary under test. The binary must be built with {@code --features
 * write-support}; its path is supplied via {@code -Dcqlite.bin} or
 * {@code $CQLITE_BIN} (the Gradle build sets this from a {@code cargo build}).
 */
public final class CqliteCompactionRunner
{
    private static final long TIMEOUT_MINUTES = 10;

    private final Path binary;

    public CqliteCompactionRunner(Path binary)
    {
        this.binary = binary;
    }

    /**
     * Resolve the cqlite binary from {@code -Dcqlite.bin} or {@code $CQLITE_BIN}.
     *
     * @throws IllegalStateException if unset or not executable — a parity run with
     *         no binary under test is a configuration error, not a silent skip.
     */
    public static CqliteCompactionRunner fromConfig()
    {
        String configured = System.getProperty("cqlite.bin");
        if (configured == null || configured.isBlank())
            configured = System.getenv("CQLITE_BIN");
        if (configured == null || configured.isBlank())
            throw new IllegalStateException(
                "cqlite binary not configured: set -Dcqlite.bin or $CQLITE_BIN to a `cqlite` " +
                "binary built with `cargo build --features write-support`.");
        Path bin = Path.of(configured);
        if (!Files.isExecutable(bin))
            throw new IllegalStateException("cqlite binary is not executable: " + bin);
        return new CqliteCompactionRunner(bin);
    }

    /** Outcome of one {@code cqlite compact} invocation. */
    public static final class Result
    {
        public final int exitCode;
        public final String stdout;
        public final String stderr;
        /** The exact argv used to launch the process (for artifact capture). */
        public final List<String> command;

        Result(int exitCode, String stdout, String stderr, List<String> command)
        {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
            this.command = List.copyOf(command);
        }

        public boolean succeeded()
        {
            return exitCode == 0;
        }

        /** The exact command line as a single shell-style string. */
        public String commandLine()
        {
            return String.join(" ", command);
        }
    }

    /**
     * Run {@code cqlite compact <inputDir> -o <outputDir> --schema <schemaFile>
     * [--gc-before N] [--now-sec N] --generation G}.
     *
     * @param gcBefore gc_grace cutoff (seconds since epoch), or {@code null} to omit
     * @param nowSec   TTL "now" (seconds since epoch), or {@code null} to omit
     */
    public Result compact(Path inputDir,
                          Path outputDir,
                          Path schemaFile,
                          Long gcBefore,
                          Long nowSec,
                          long generation) throws IOException, InterruptedException
    {
        List<String> cmd = new ArrayList<>();
        cmd.add(binary.toString());
        cmd.add("compact");
        cmd.add(inputDir.toString());
        cmd.add("--output");
        cmd.add(outputDir.toString());
        cmd.add("--schema");
        cmd.add(schemaFile.toString());
        if (gcBefore != null)
        {
            cmd.add("--gc-before");
            cmd.add(Long.toString(gcBefore));
        }
        if (nowSec != null)
        {
            cmd.add("--now-sec");
            cmd.add(Long.toString(nowSec));
        }
        cmd.add("--generation");
        cmd.add(Long.toString(generation));

        // Redirect to files rather than reading the pipes inline: a child that fills
        // either pipe buffer while we block reading the other would deadlock.
        Path outFile = Files.createTempFile("cqlite-compact-out", ".log");
        Path errFile = Files.createTempFile("cqlite-compact-err", ".log");
        try
        {
            Process proc = new ProcessBuilder(cmd)
                           .redirectOutput(outFile.toFile())
                           .redirectError(errFile.toFile())
                           .start();
            if (!proc.waitFor(TIMEOUT_MINUTES, TimeUnit.MINUTES))
            {
                proc.destroyForcibly();
                throw new IOException("cqlite compact timed out after " + TIMEOUT_MINUTES + "m: " + cmd);
            }
            return new Result(proc.exitValue(),
                              Files.readString(outFile),
                              Files.readString(errFile),
                              cmd);
        }
        finally
        {
            Files.deleteIfExists(outFile);
            Files.deleteIfExists(errFile);
        }
    }
}
