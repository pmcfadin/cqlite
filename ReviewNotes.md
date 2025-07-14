**PRD.md**

This will be an Apache licensed OSS project and not a commercial one. It will live in github and eventually be donated the the Apache Cassandra project. 

This is a project that has been though through by Apache Cassandra PMC member Patrick McFadin. It encapsulates a lot of his 13 years of expirience and holds true to the ideals of Cassandra

Instead of being backward compatible with versions less than 5, we will just start here. 

We can skip adoption metrics. 

Skip the marketing section

**ARCHITECTURE_DIAGRAM.md**

 Query parser needs to parse CQL not SQL

 Avoid the complexity of multiple SSTables and everything that includes. No compaction or things like bloomfilters. CQLite should be reading and writing to one data file and it's associated support files.

 Rethink the architecture with this very important change in mind.

 **ARCHITECTURE.md**

 Same issue as with the Architecure Diagram. Simplify all file operations by having only one file per table in a similar file directory layout as Cassandra. The only excpetion being just one data file. 

 **RD_ROADMAP.md**

Invlode the end users for important data creation. To build a file reader, writer and parser you know works with Cassandra 5, you need actual Cassandra 5 data tables from a working system. To do that, specify the actions an end user should take OR use docker and agents to create your own "gold master" data file creation engine. In either case, you will need to test different schemas and reserved words. 

You will need to understand the CQL 3 grammar very thoroughly. Patrick has done some conversion work to get it into Antlr4 which you can find here: https://github.com/pmcfadin/cassandra-antlr4-grammar

In intermediate deliverable to allow user acceptance testing of parsing efforts will be a very simple command line tool. Give it the ability to exercise whichever path you are currently researching to get feedback on implementation. 

Make a document that tracks major and minor milestones so they can be tracked by observers of the project. 

**Overall comments**

Use git to commit often. Use the gh commandline to work with teh current project. Use Github issues to track progress. Create and assign proper labels for the aspects of the project that are important. Use the README to communicate with the outside world watching the progress. 