Suggested fields — Project: CASSANDRA / Type: Improvement / Component: Local/Compaction
Title: Complete cursor compaction coverage (complex columns, BTI, wide schemas), verified byte-for-byte against the iterator path

Body below in JIRA wiki markup — paste everything after this line.
----

h2. Background

CASSANDRA-20918 introduced cursor-based compaction ({{CursorCompactor}}, {{SSTableCursorReader}}, {{SSTableCursorWriter}}): a streaming merge that avoids the per-partition/row/cell object materialization of the iterator path ({{CompactionIterator}}). Reported results: 2–5x faster compaction, up to ~100x allocation reduction, with a fixed memory footprint per sstable.

The motivating problem is CASSANDRA-20428: {{ByteArrayAccessor.read}} — the {{new byte[length]}} per value performed by cell and clustering deserialization — is a dominant allocation source wherever the iterator-based read machinery runs, and compaction pays it for every cell of every sstable it merges. The cursor path eliminates that allocation class entirely, but only for the schemas and configurations it supports; everything else silently falls back to the iterator path at {{AbstractCompactionPipeline.create}}.

h2. What was missing

As merged, {{CursorCompactor.isSupported()}} / {{unsupportedMetadata()}} reject most real-world schemas and configurations, so the allocation and throughput wins do not apply where they matter most:

# *Multi-cell columns* — non-frozen collections and UDTs are unsupported. Any table with a {{map}}, {{set}}, {{list}}, or non-frozen UDT column falls back.
# *BTI output format* — the cursor only writes the BIG format. Clusters on the trie format (the trunk default direction) never use the cursor path.
# *Partial-range scanners* — token-subrange compactions fall back.
# *Counters* — counter tables fall back.
# *Garbage-skipping modes* — {{tombstoneOption != NONE}} falls back (default is NONE, so this is lower priority).

Out of scope, by design: secondary indexes / SAI (index observers consume materialized rows and need their own design) and pre-current sstable versions (compaction rewrites to the current version, so the gap phases out on its own).

Equally missing was *verification*: the existing cursor tests asserted each path's output against expected CQL results independently, but never compared the two paths against each other, and never at the file level. Two compaction implementations that must be interchangeable at scale need a stronger bar than "both look right in isolation" — divergence between cursor- and iterator-compacted replicas is silent and permanent.

h2. What needs to be implemented

Close the support gaps incrementally — complex columns, then BTI output, then partial-range scanners, then counters, with the garbage-skipper equivalent optional — with each increment landing only when it passes the verification bar below. Wide schemas (>64-column subset encodings), materialized-view tables (strict liveness), and multi-gigabyte partitions are part of the supported surface and must be covered, not special-cased away.

Two standing constraints:

* *Garbage-free is the point of the feature.* Every change must preserve the no-per-element-allocation property of the hot path, enforced by measurement rather than code review alone.
* *Behavior must be identical, not merely equivalent.* Purge decisions, tie-breaks, stats, index structure — everything.

h2. Required: byte-for-byte differential verification

The two compaction implementations must be interchangeable, and the way to prove that is a test harness that compacts the same inputs through both production pipelines and compares the outputs *byte for byte* — every component of every output sstable, in both sstable formats, with no exception mechanism: any divergence is a failure. (Experience so far supports the bar: every byte divergence found this way has been a bug, never a justified difference.)

Coverage should span the supported surface — the tombstone, TTL, and timestamp edge cases where merge implementations classically disagree, randomized schemas and workloads, and large-scale shapes (wide schemas, many-sstable merges, multi-gigabyte partitions) — and should grow with each increment so a closed gap can never silently fall back or regress. The harness design beyond that is an implementation detail of the patch.
