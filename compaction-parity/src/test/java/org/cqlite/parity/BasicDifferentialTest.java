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

import java.util.List;

import org.junit.Test;

/**
 * First differential scenario: live rows across two overlapping SSTables.
 *
 * <p>The simplest meaningful merge — two SSTables that overlap on some primary
 * keys, where the newer write must win (last-write-wins). No tombstones, so no
 * purging is involved; this is the harness's green smoke and proves the whole
 * pipeline (build inputs → reference compaction → cqlite compaction → logical
 * diff) end-to-end. Tombstone / TTL / collection scenarios follow (#844–#853).
 */
public class BasicDifferentialTest extends DifferentialParityTester
{
    @Test
    public void liveRowsLastWriteWinsAcrossTwoSSTables() throws Exception
    {
        String ddl = "CREATE TABLE %s (id int, ck int, v text, PRIMARY KEY (id, ck))";

        // SSTable A (ts=1000): ids 1..3, each one clustering row.
        List<String> a = List.of(
            "INSERT INTO %s (id, ck, v) VALUES (1, 0, 'a-1') USING TIMESTAMP 1000",
            "INSERT INTO %s (id, ck, v) VALUES (2, 0, 'a-2') USING TIMESTAMP 1000",
            "INSERT INTO %s (id, ck, v) VALUES (3, 0, 'a-3') USING TIMESTAMP 1000");

        // SSTable B (ts=2000): overrides id 2 and 3, adds id 4.
        List<String> b = List.of(
            "INSERT INTO %s (id, ck, v) VALUES (2, 0, 'b-2') USING TIMESTAMP 2000",
            "INSERT INTO %s (id, ck, v) VALUES (3, 0, 'b-3') USING TIMESTAMP 2000",
            "INSERT INTO %s (id, ck, v) VALUES (4, 0, 'b-4') USING TIMESTAMP 2000");

        assertCqliteMatchesCassandra(ddl, List.of(a, b));
    }

    /**
     * No-clustering variant: a partition-key-only table (one row per partition).
     * cqlite round-trips these (see compact_command.rs); this asserts Cassandra
     * agrees, i.e. the harness confirms a genuine MATCH, not just catches mismatches.
     */
    @Test
    public void liveRowsLastWriteWinsNoClustering() throws Exception
    {
        String ddl = "CREATE TABLE %s (id int PRIMARY KEY, v text)";

        List<String> a = List.of(
            "INSERT INTO %s (id, v) VALUES (1, 'a-1') USING TIMESTAMP 1000",
            "INSERT INTO %s (id, v) VALUES (2, 'a-2') USING TIMESTAMP 1000",
            "INSERT INTO %s (id, v) VALUES (3, 'a-3') USING TIMESTAMP 1000");

        List<String> b = List.of(
            "INSERT INTO %s (id, v) VALUES (2, 'b-2') USING TIMESTAMP 2000",
            "INSERT INTO %s (id, v) VALUES (3, 'b-3') USING TIMESTAMP 2000",
            "INSERT INTO %s (id, v) VALUES (4, 'b-4') USING TIMESTAMP 2000");

        assertCqliteMatchesCassandra(ddl, List.of(a, b));
    }
}
