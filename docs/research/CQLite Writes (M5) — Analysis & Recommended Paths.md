CQLite Writes (M5) — Analysis & Recommended Paths

Goal context: You’re building CQLite, a Rust library that can read Cassandra SSTables today and wants to write in a way that (a) works well as an embedded/library workflow and (b) can ultimately produce SSTables that load into a real Cassandra cluster and work (PRD M5: “Generate valid Cassandra 5 SSTables”).

This document:
	•	Critiques the current brainstormed directions.
	•	Clarifies what “works in Cassandra” truly requires.
	•	Proposes new options that better match the constraints.
	•	Recommends a staged plan with clear validation gates.

⸻

1) Problem framing: embedded/library writes vs Cassandra SSTables

You’re balancing two separate systems:
	1.	Embedded/library write experience
	•	Predictable memory.
	•	No always-on daemon required.
	•	Good crash safety.
	•	Reasonable read performance without large compactions happening “surprise!”
	2.	Cassandra interoperability
	•	The output must be canonical SSTables that Cassandra tooling will accept.
	•	Ideal workflow is to generate SSTables and ingest using known-safe tooling (e.g., streaming bulk load) rather than filesystem-copy hacks.

These goals are compatible, but only if you separate:
	•	Internal ingestion representation (can be optimized for library UX)
from
	•	Export artifact (must be canonical Cassandra SSTables)

A key decision: Do you want the on-disk structure to be a “real SSTable” all the time, or is it okay to have a library-native durable representation that is later exported into real SSTables?

⸻

2) Non-negotiables for “SSTables work in Cassandra”

If you want Cassandra to accept the SSTables you generate, you are signing up for these invariants:

2.1 Canonical sort order

A Cassandra SSTable Data.db is not just “data written to disk.” It is ordered:
	•	Partition ordering: by decorated key (token order), then by key bytes
	•	Row ordering within partition: by clustering comparator (clustering key order)

If you store unsorted rows/partitions and “index” them, that is no longer a Cassandra SSTable.

2.2 Component set and structure

A usable SSTable is a set of components (files) with expected structure, checksums, and metadata.
At minimum you will need to emit:
	•	Data.db
	•	Index.db (or BTI equivalents)
	•	Filter.db (Bloom filter)
	•	Statistics.db
	•	TOC.txt
	•	Digest / checksums (implementation varies by version/config)
	•	(If compressed) CompressionInfo.db

2.3 Format selection

Cassandra 5 introduced BTI (trie-indexed SSTables), but it still supports legacy BIG format.
For M5, you should assume:
	•	Implement BIG writer first to reduce complexity.
	•	Add BTI later once correctness is proven.

2.4 Ingestion path

The safest, most realistic workflow is:
	•	Generate valid SSTables
	•	Bulk-load into a cluster using standard tooling

Do not rely on “copy files into a node data directory” as your primary story; it’s brittle and operationally hostile.

⸻

3) Critique of current doc’s three directions

Direction A: “Segmented Log + persistent SkipList index pointing to unsorted Data”

What it does well (embedded):
	•	Fast append/write.
	•	Crash recovery is straightforward (log semantics).
	•	Index can support point lookups quickly.

Why it conflicts with Cassandra interoperability:
	•	The doc notes it is not byte-compatible with Cassandra Data.db (requires converter).
	•	That’s the big truth: unsorted Data is not a Cassandra SSTable.

Hidden complexity / risk:
	•	Partition range reads and clustering slice queries become painful without a sorted layout.
	•	Export/conversion becomes the real write engine, and that part must be perfect.

Verdict:
	•	Direction A is valid only if you declare it as an internal ingest format plus an explicit export step.
	•	It is not “SSTable writing” in the Cassandra sense.

⸻

Direction B: “Memory-mapped persistent sorted structure”

What it does well:
	•	Potentially low-latency reads/writes with less copying.
	•	Good for embedded use if done right.

Why it’s risky in Rust:
	•	You’re implementing a persistent memory discipline:
	•	crash-consistent commits
	•	copy-on-write or journaling
	•	pointer/offset safety
	•	strict versioning + migration
	•	mmap + msync durability semantics can be subtle across OS/filesystems.

Verdict:
	•	Only worth it if “embedded mutable store” is a first-class product goal.
	•	It’s not the fastest path to Cassandra-compatible SSTables.

⸻

Direction C: “Lazy LSM, compaction triggered on reads/writes (foreground)”

What it does well:
	•	Naturally produces real sorted SSTables.
	•	Aligns best with “work in Cassandra.”

Main failure mode:
	•	User-visible latency spikes if compaction happens implicitly.

Verdict:
	•	Direction C is the right spine, but you should redesign the operational model to be explicit rather than “surprise compaction.”

⸻

4) New options that better match your constraints

Option D: External-sort SSTable Builder (two-phase: runs → merge)

This is the most “library-native” way to produce canonical SSTables without background daemons.

Workflow:
	1.	Accept writes in arbitrary order.
	2.	Buffer into a bounded in-memory structure (memtable-like).
	3.	When threshold reached, sort that buffer by Cassandra order and write a sorted run (a mini-SSTable or intermediate run).
	4.	Periodically perform a k-way merge of runs into a final canonical SSTable.

Why it’s great:
	•	Predictable memory cap.
	•	Mostly sequential IO.
	•	No background thread required; merges happen when asked.
	•	Output is Cassandra-correct.

Key design choice:
	•	Runs can be:
	•	(a) real SSTables already (tiered SSTables)
	•	(b) a simpler intermediate run format that you merge into a final SSTable.

⸻

Option E: “Ingest Log (A) + Deterministic Export to SSTables”

Keep Direction A for embedded durability, but explicitly separate:
	•	Library-native durable ingest state
from
	•	Export job that produces canonical SSTables.

Why it can be ideal:
	•	Ingest path is simple and robust.
	•	Export path is explicit and testable.
	•	You can schedule export on-demand (or by policy).

Downside:
	•	You must build and maintain the exporter anyway, and it becomes the core correctness surface.

⸻

Option F: “Golden writer backend” (Java reference) + Rust writer later

If you want early correctness and faster path to “loads in Cassandra,” you can temporarily rely on Java’s known-good SSTable generation paths and focus Rust on orchestration.

Shapes:
	•	Out-of-process helper (sidecar) that receives rows and emits SSTables.
	•	JNI bridge.

Use case:
	•	As a transitional strategy or as a validation oracle for Rust output.

Downside:
	•	Complexity of packaging.
	•	Reduces purity of “all Rust.”

⸻

5) Recommendation: best path if Cassandra interoperability is #1

5.1 Choose BIG writer first

Start with BIG format rather than BTI.
	•	Less complexity.
	•	Broad compatibility.
	•	BTI later as an optimization.

5.2 Implement Option D as the primary architecture

External-sort builder + explicit maintenance APIs.

Do not do “compaction on read.” Instead:
	•	Provide explicit APIs for maintenance work.
	•	Allow users to opt in to automatic policies.

Suggested public API (Rust sketch):
	•	write(mutation)
	•	flush_run() → write a sorted run (and/or a level-0 SSTable)
	•	maintenance_step(budget_ms) → merge/compact incrementally within a time budget
	•	export_sstable(target_dir) → finalize into canonical SSTables suitable for Cassandra ingestion
	•	stats() → expose run count, read-amp estimate, bytes pending merge

User experience:
	•	Deterministic latency: user chooses when to spend IO.

⸻

6) Concrete architecture proposal (modules and responsibilities)

6.1 Core data model
	•	SchemaDescriptor:
	•	partition key types
	•	clustering key types
	•	column metadata
	•	comparator behavior / byte encoding rules
	•	Mutation:
	•	partition key
	•	clustering key
	•	column updates
	•	timestamps
	•	tombstones / TTL metadata (as you expand scope)
	•	ComparableKey:
	•	decorated key = token + key bytes (or key bytes + token computed)
	•	supports total ordering needed for SSTable writing

6.2 In-memory buffer (Memtable-like)
	•	Backed by BTreeMap or custom arena + sort vector
	•	Keyed by:
	•	partition decorated key
	•	clustering key
	•	Threshold policies:
	•	bytes
	•	row count
	•	partitions

6.3 Run writer

Two variants:

Variant 1: Run = real SSTable (Level 0)
	•	When memtable flushes, you write a valid SSTable immediately.
	•	This simplifies export (runs are already SSTables).
	•	Merging runs is like compaction.

Variant 2: Run = intermediate format
	•	Write a simpler sequential format optimized for merge.
	•	Later merge into canonical SSTable.

Recommendation: start with Variant 1 if you can; it reduces “format count”.

6.4 Merge / compaction engine
	•	Merges multiple sorted runs into one sorted output.
	•	Can be implemented as:
	•	k-way merge iterators
	•	streaming writer that emits Data.db + builds indices
	•	Policies:
	•	“merge when run count exceeds N”
	•	“merge when read amp > X”
	•	“user calls maintenance_step”

6.5 SSTable component writers

Build these while streaming the final Data.db:
	•	Partition index writer
	•	Row index (if needed by format)
	•	Bloom filter builder
	•	Statistics collector
	•	Compression support (later)
	•	TOC and digests

⸻

7) Staged implementation plan (practical, testable)

Stage 0: Minimum viable writer output (uncompressed BIG)
	•	Support a narrow schema subset:
	•	single partition key
	•	simple clustering
	•	basic scalar columns
	•	fixed timestamp rule
	•	Emit full SSTable component set required for Cassandra to read.

Stage 1: Validate on real Cassandra
	•	Start Cassandra locally (or in CI).
	•	Use the canonical ingestion tool path.
	•	Query via CQL to validate:
	•	row counts
	•	key order
	•	slice reads

Stage 2: Expand data model features

Add incrementally:
	•	multiple clustering columns
	•	static rows
	•	TTL
	•	tombstones
	•	collections / UDTs (later)

Stage 3: Add merge policy + incremental maintenance
	•	Implement maintenance_step(budget)
	•	Provide user-visible knobs and metrics.

Stage 4: Optional BTI writer
	•	Replace primary index structures with BTI equivalents.
	•	Keep component writing modular so BIG→BTI is a swap, not a rewrite.

⸻

8) Validation strategy (this is where projects succeed or die)

8.1 Golden/reference comparisons

You need a “ground truth” harness so you don’t spend weeks debugging subtle invariants.

Approaches:
	•	Generate equivalent SSTables with a reference generator (Java path) and compare:
	•	count
	•	partition boundaries
	•	metadata sanity
	•	Use offline validators:
	•	dump components
	•	verify checksums

8.2 End-to-end cluster ingest test

Each CI run should include at least one dataset where you:
	•	generate SSTables
	•	ingest into Cassandra
	•	run a deterministic query suite

Include datasets with:
	•	multiple partitions
	•	multiple clustering rows
	•	overlapping updates

⸻

9) Decision matrix: which option when?

If Cassandra interoperability is the priority:
	•	Option D (external-sort builder) + BIG first

If embedded UX is priority and export is acceptable:
	•	Option E (ingest log + export)

If you want quickest path to correctness:
	•	Option F (golden writer backend) as a bridge

⸻

10) Bottom line recommendation

Do this:
	1.	Implement BIG SSTable writer first.
	2.	Structure the write path as Option D with explicit maintenance.
	3.	Build a hard validation loop against real Cassandra ingestion.
	4.	Add BTI only after correctness is boring.

This gives you:
	•	Library-friendly writes without background daemons.
	•	Deterministic performance controls.
	•	The strongest path to “SSTables load into Cassandra and work.”