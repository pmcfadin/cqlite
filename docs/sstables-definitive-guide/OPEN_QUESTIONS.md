# Open Questions and Scoping Decisions

- Version scope: lock to 5.1? Include deltas for 4.1 and 3.11?
A: Our focuse on details should be on Cassandra 5.0. 3.x and 4.x need to be addressed in the background and evolution of formats. I think it would also be good to have callouts in oreilly style when significant differences between 5 and previosu versions arise. 

- Depth on BTI: full chapter vs appendix-only?
A: BTI should be a chapter or at least a significant portion of a chapter. 

- SAI: capture on-disk per-version changes and segment formats across releases
A: This is a definitive guide. Let's get into the details and maybe a diagram here too. 

- Include repair/streaming internals or keep to SSTable artifacts only?
A: Reapir and streaming need a metion as a process that sstables require as part of being in a cluster. However, these do not need detailed guides or explaination. They use the read and write path which are already explained. 

- Coverage of encryption at rest and key management specifics
A: Out of scope for this guide. Those are node operation topics. 

- Include performance tuning defaults and recommended configs by workload
A: Out of scope for this guide. Those are operation topics

- Diagrams stack: use Mermaid or embed pre-rendered SVGs?
A: Mermaid is a good start. If we need to use SVG that will be a later iteration.

- Testing samples: which datasets from `test-data/` to canonicalize in examples?
A: The "test_basic" dataset should be a good representative. 

# New Clarifying Questions
- Confirm Cassandra 5.0 vs 5.1 references preference (pin to 5.1 when code stable?)
A: 5.0. There may not be a 5.1 The project is looking at the next version as 6.0

- SAI: preferred minimum version baseline and segment format references to cite
A: Wjatever shipps in 5, but again, lets do callouts to previous versions. 

- BTI: select one canonical workload example to contrast with big/mc/mm
A: I don't have one off hand. We'll need to find one. 

- Diagrams: repository for exported SVGs (keep in-tree under `diagrams/`)?
A: If we have expoerted SVG, they would go in diagrams. 

- Code permalinks: pin to specific Cassandra git tags for long-term stability?
A: Yes, make sure there are clean, permanent links. 


## Further Clarifying Questions
- Pinning target: do you prefer `cassandra-5.0.0` tag specifically, or `cassandra-5.0` branch for permalinks?
A: cassandra-5.0.0 branch since it pins a version. 

- SAI scope: include numeric and text index internals only, or also cover spatial/vector if present in 5.0?
A: Include vector since it is a part of 5.0 

- BTI exemplar: shall we generate a small canonical dataset/workload to illustrate differences, or reference a public one if available?
A: 

- Diagram tooling: prefer `mermaid-cli` export committed to `.svg`, or editor-based export is fine as long as `.mmd` is present?
A: Just keep it at mermaid for now. Don't make this complicated

- External code policy: avoid embedding upstream source in the book and link via permalinks only?
A: Embed external code snippets with a permalink to full source. Do not put large amouns of code if possible. 

- Utilities output: okay to trim `sstabledump`/`sstablemetadata` outputs to the relevant sections with annotations?
A: Keep it small and readable. Your job is to get the point accross not swamp people with text. Be consise. 

- Compaction strategies: include UCS details alongside STCS/LCS/TWCS, or keep UCS as a sidebar?
A: Since UCS is a derrivite, keep it in a sidebar. Compaction in this book is an important concept, not the primary focus.
