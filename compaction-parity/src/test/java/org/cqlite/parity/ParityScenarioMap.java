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

/**
 * Maps a differential-harness test method onto the parity-manifest scenario it
 * exercises (issue #1027, task 3.1). The scenario id is the join key back to
 * {@code test-data/cassandra-parity-manifest.yml}; a red gate maps mechanically
 * to its {@code cass.*} scenario via the failure bundle directory name.
 *
 * <p>There is no auto-derivable mapping from a JUnit method to a manifest id, so
 * this is an EXPLICIT, minimal table. Every {@code BasicDifferentialTest} method
 * maps to two manifest scenarios depending on the asserting tier:
 *
 * <ul>
 *   <li><b>LOGICAL</b> ({@code gradle test}, tier {@code required_parity},
 *       {@code canonical_semantic}): the sstabledump-equality gate, manifest
 *       scenario {@code cass.compaction.CompactionIteratorTest.differential_compaction_loop}.</li>
 *   <li><b>BYTE</b> ({@code gradle byteParity}, tier {@code nightly_docker},
 *       {@code byte_for_byte}): the per-component byte gate whose MECHANISM
 *       scenario is {@code cass.compaction.harness_byte_tier_artifacts}
 *       (BasicDifferentialTest + the byte utilities are that scenario's coverage).</li>
 * </ul>
 *
 * <p>Unknown methods fall back to the default scenario for the active tier so a
 * newly added scenario still produces a conforming (if generically-keyed) bundle
 * rather than crashing the harness; the fallback id is also a valid manifest id.
 */
final class ParityScenarioMap
{
    /** Manifest scenario for the LOGICAL (canonical_semantic / required_parity) tier. */
    static final String LOGICAL_SCENARIO =
        "cass.compaction.CompactionIteratorTest.differential_compaction_loop";
    static final String LOGICAL_TIER = "required_parity";
    static final String LOGICAL_EVIDENCE = "canonical_semantic";

    /** Manifest scenario for the BYTE (byte_for_byte / nightly_docker) tier. */
    static final String BYTE_SCENARIO = "cass.compaction.harness_byte_tier_artifacts";
    static final String BYTE_TIER = "nightly_docker";
    static final String BYTE_EVIDENCE = "byte_for_byte";

    private ParityScenarioMap() {}

    /** Resolution of a test method to its manifest scenario for one tier. */
    static final class Resolution
    {
        final String scenarioId;
        final String tier;
        final String evidenceType;

        Resolution(String scenarioId, String tier, String evidenceType)
        {
            this.scenarioId = scenarioId;
            this.tier = tier;
            this.evidenceType = evidenceType;
        }
    }

    /**
     * Resolve the manifest scenario for {@code Class.method}. {@code byteTier}
     * selects between the byte and logical manifest scenarios (both harness
     * tiers run the SAME BasicDifferentialTest methods).
     *
     * @param classDotMethod e.g. {@code BasicDifferentialTest.liveRowsLastWriteWinsAcrossTwoSSTables}
     * @param byteTier       true when the byte tier is the asserting tier (byteParity)
     */
    static Resolution resolve(String classDotMethod, boolean byteTier)
    {
        // The mapping is intentionally tier-driven, not per-method: every method
        // in the differential suite proves the same harness scenario, and the
        // manifest models the harness as one logical + one byte scenario. Keep an
        // explicit switch here so adding a method that maps to a NEW manifest
        // scenario is a one-line, reviewable change.
        switch (classDotMethod)
        {
            case "BasicDifferentialTest.liveRowsLastWriteWinsAcrossTwoSSTables":
            case "BasicDifferentialTest.liveRowsLastWriteWinsNoClustering":
            default:
                return byteTier
                       ? new Resolution(BYTE_SCENARIO, BYTE_TIER, BYTE_EVIDENCE)
                       : new Resolution(LOGICAL_SCENARIO, LOGICAL_TIER, LOGICAL_EVIDENCE);
        }
    }
}
