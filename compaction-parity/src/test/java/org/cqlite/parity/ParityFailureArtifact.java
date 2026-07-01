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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The uniform parity failure-artifact record ({@code failure-artifact.json})
 * emitted by the compaction Java harness (issue #1027, task 3.1/3.2).
 *
 * <p>This is the Java counterpart of the Rust emitter
 * {@code tools/cassandra-parity/src/failure_artifact.rs}: it produces the SAME
 * JSON shape validated by {@code test-data/parity-failure-artifact.schema.json}
 * so all parity surfaces (Rust required_parity checks + this harness + the live
 * lanes) emit an identical, machine-checkable record.
 *
 * <p>The record joins back to the parity manifest via {@link #scenarioId}, and
 * is persisted inside a scenario-id-keyed bundle
 * {@code <root>/parity-failures/<tier>/<scenario_id>/} (see
 * {@link ParityFailureBundle}).
 *
 * <p>A tiny hand-rolled JSON writer keeps this class free of any external JSON
 * dependency (the harness classpath is Cassandra + JUnit only). The field order
 * and names match the schema's {@code required} list exactly.
 */
final class ParityFailureArtifact
{
    static final int SCHEMA_VERSION = 1;
    static final String RECORD_FILE_NAME = "failure-artifact.json";

    /** One {@code diffs[]} pointer, typed by what was compared. */
    static final class Diff
    {
        final String kind; // a FAILURE_ARTIFACT_KIND value
        final String path; // relative to the bundle dir

        Diff(String kind, String path)
        {
            this.kind = kind;
            this.path = path;
        }
    }

    /** The full reproduction context (mirrors the Rust {@code Provenance}). */
    static final class Provenance
    {
        String cassandraVersion;
        String cassandraGitSha;
        String datasetSha256;
        String fixturePath;
        List<String> componentList = new ArrayList<>();
        String commandLine;
        String stdout = "stdout.txt";
        String stderr = "stderr.txt";
    }

    String scenarioId;
    String lane;
    String tier;
    String evidenceType;
    List<String> artifactsCompared = new ArrayList<>();
    final Provenance provenance = new Provenance();
    final List<Diff> diffs = new ArrayList<>();
    String reproBundle = "repro/";

    /** Serialize to pretty JSON matching the failure-artifact schema. */
    String toJson()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        appendField(sb, 1, "schema_version", SCHEMA_VERSION);
        appendComma(sb);
        appendField(sb, 1, "scenario_id", scenarioId);
        appendComma(sb);
        appendField(sb, 1, "lane", lane);
        appendComma(sb);
        appendField(sb, 1, "tier", tier);
        appendComma(sb);
        appendField(sb, 1, "evidence_type", evidenceType);
        appendComma(sb);
        appendStringArray(sb, 1, "artifacts_compared", artifactsCompared);
        appendComma(sb);
        appendProvenance(sb, 1);
        appendComma(sb);
        appendDiffs(sb, 1);
        appendComma(sb);
        appendField(sb, 1, "repro_bundle", reproBundle);
        sb.append("\n}\n");
        return sb.toString();
    }

    private void appendProvenance(StringBuilder sb, int indent)
    {
        indent(sb, indent);
        sb.append("\"provenance\": {\n");
        appendField(sb, indent + 1, "cassandra_version", provenance.cassandraVersion);
        appendComma(sb);
        appendField(sb, indent + 1, "cassandra_git_sha", provenance.cassandraGitSha);
        appendComma(sb);
        appendField(sb, indent + 1, "dataset_sha256", provenance.datasetSha256);
        appendComma(sb);
        appendField(sb, indent + 1, "fixture_path", provenance.fixturePath);
        appendComma(sb);
        appendStringArray(sb, indent + 1, "component_list", provenance.componentList);
        appendComma(sb);
        appendField(sb, indent + 1, "command_line", provenance.commandLine);
        appendComma(sb);
        appendField(sb, indent + 1, "stdout", provenance.stdout);
        appendComma(sb);
        appendField(sb, indent + 1, "stderr", provenance.stderr);
        sb.append('\n');
        indent(sb, indent);
        sb.append('}');
    }

    private void appendDiffs(StringBuilder sb, int indent)
    {
        indent(sb, indent);
        sb.append("\"diffs\": [");
        if (diffs.isEmpty())
        {
            sb.append(']');
            return;
        }
        sb.append('\n');
        for (int i = 0; i < diffs.size(); i++)
        {
            Diff d = diffs.get(i);
            indent(sb, indent + 1);
            sb.append("{\n");
            appendField(sb, indent + 2, "kind", d.kind);
            appendComma(sb);
            appendField(sb, indent + 2, "path", d.path);
            sb.append('\n');
            indent(sb, indent + 1);
            sb.append('}');
            if (i < diffs.size() - 1)
                sb.append(',');
            sb.append('\n');
        }
        indent(sb, indent);
        sb.append(']');
    }

    // ── JSON primitives ──────────────────────────────────────────────────────

    private static void appendField(StringBuilder sb, int indent, String key, int value)
    {
        indent(sb, indent);
        sb.append('"').append(key).append("\": ").append(value);
    }

    private static void appendField(StringBuilder sb, int indent, String key, String value)
    {
        indent(sb, indent);
        sb.append('"').append(key).append("\": ").append(quote(value));
    }

    private static void appendStringArray(StringBuilder sb, int indent, String key, List<String> values)
    {
        indent(sb, indent);
        sb.append('"').append(key).append("\": [");
        for (int i = 0; i < values.size(); i++)
        {
            sb.append(quote(values.get(i)));
            if (i < values.size() - 1)
                sb.append(", ");
        }
        sb.append(']');
    }

    private static void appendComma(StringBuilder sb)
    {
        sb.append(",\n");
    }

    private static void indent(StringBuilder sb, int levels)
    {
        for (int i = 0; i < levels; i++)
            sb.append("  ");
    }

    /** JSON string literal with the minimal required escapes. */
    static String quote(String s)
    {
        if (s == null)
            return "\"\"";
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);
            switch (c)
            {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:
                    if (c < 0x20)
                        sb.append(String.format("\\u%04x", (int) c));
                    else
                        sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }

    /** Write the record as {@code failure-artifact.json} inside {@code bundleDir}. */
    Path writeTo(Path bundleDir) throws IOException
    {
        Files.createDirectories(bundleDir);
        Path recordPath = bundleDir.resolve(RECORD_FILE_NAME);
        Files.writeString(recordPath, toJson(), StandardCharsets.UTF_8);
        return recordPath;
    }

    /** SHA-256 hex of a file, or a 64-zero placeholder if it cannot be read. */
    static String sha256(Path file)
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
        catch (IOException | NoSuchAlgorithmException e)
        {
            return "0".repeat(64);
        }
    }

    /** Deterministic sorted component-kind list of an output dir (for provenance). */
    static List<String> componentKinds(Path dir)
    {
        List<String> kinds = new ArrayList<>();
        if (dir == null || !Files.isDirectory(dir))
            return kinds;
        try (var stream = Files.list(dir))
        {
            Map<String, Boolean> unique = new LinkedHashMap<>();
            stream.filter(Files::isRegularFile)
                  .map(p -> p.getFileName().toString())
                  .map(ParityFailureArtifact::componentKind)
                  .sorted()
                  .forEach(k -> unique.put(k, Boolean.TRUE));
            kinds.addAll(unique.keySet());
        }
        catch (IOException ignored)
        {
            // Best-effort provenance; a missing inventory must not mask the failure.
        }
        return kinds;
    }

    /** The component "kind" is the filename token after the last dash. */
    static String componentKind(String fileName)
    {
        int dash = fileName.lastIndexOf('-');
        return dash >= 0 ? fileName.substring(dash + 1) : fileName;
    }
}
