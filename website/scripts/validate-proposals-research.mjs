import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;

const checks = [
  {
    name: 'sidebar has Proposals and Research section',
    file: 'astro.config.mjs',
    includes: ["label: 'Proposals and Research'", "directory: 'proposals-research'"],
  },
  {
    name: 'home page links to proposals hub',
    file: 'src/content/docs/index.mdx',
    includes: ['title="Proposals and Research"', 'href="/cqlite/proposals-research/"'],
  },
  {
    name: 'proposals hub exists and features all research topics',
    file: 'src/content/docs/proposals-research/index.mdx',
    includes: [
      'title: Proposals and Research',
      'Storage Engine Direction',
      'Memtable Freshness',
      'Lakehouse Materialization',
      'Cassandra Seam Inventory',
      'Read Path & Query Providers',
      'Compaction & Maintenance',
      '/cqlite/proposals-research/storage-engine/',
      '/cqlite/proposals-research/memtable-freshness/',
      '/cqlite/proposals-research/iceberg-materializer/',
      '/cqlite/proposals-research/cassandra-seams/',
      '/cqlite/proposals-research/read-path-query-providers/',
      '/cqlite/proposals-research/compaction-maintenance/',
    ],
    excludes: [
      'Performance Research',
      'API, CLI & Maintenance Contracts',
      '/cqlite/proposals-research/performance-research/',
      '/cqlite/proposals-research/api-cli-maintenance-contracts/',
    ],
  },
  {
    name: 'storage-engine proposal page exists with approved parallel-plane model',
    file: 'src/content/docs/proposals-research/storage-engine.mdx',
    includes: [
      'title: Storage Engine Direction',
      'Cassandra OLTP Plane',
      'SSTable Foundation',
      'OLAP Plane',
      'storageEngineConcept',
      'docs/storage engine/report-2-storage-engine-feasibility.md',
      '```mermaid',
    ],
  },
  {
    name: 'storage-engine architecture renders Mermaid instead of static SVG',
    file: 'src/content/docs/proposals-research/storage-engine.mdx',
    includes: [
      "import MermaidDiagram from '../../../components/MermaidDiagram.astro';",
      'const architectureMermaid = `',
      '<MermaidDiagram',
      'chart={architectureMermaid}',
      'sourceLabel="Mermaid source for the architecture diagram"',
    ],
    excludes: [
      "import architectureDiagram from '../../../assets/storage-engine-architecture.svg';",
      'src={architectureDiagram.src}',
      'storage-engine-architecture.svg',
    ],
  },
  {
    name: 'storage-engine data paths render Mermaid instead of static SVG',
    file: 'src/content/docs/proposals-research/storage-engine.mdx',
    includes: [
      'const dataPathsMermaid = `',
      'chart={dataPathsMermaid}',
      'sourceLabel="Mermaid source for the data-path diagram"',
    ],
    excludes: [
      "import dataPathsDiagram from '../../../assets/storage-engine-data-paths.svg';",
      'src={dataPathsDiagram.src}',
      'storage-engine-data-paths.svg',
    ],
  },
  {
    name: 'storage-engine Mermaid diagrams use compact grouped layouts',
    file: 'src/content/docs/proposals-research/storage-engine.mdx',
    includes: [
      'export const architectureMermaid = `flowchart LR',
      'export const dataPathsMermaid = `flowchart TB',
      'Data.db / Index.db<br/>Summary.db / Statistics.db',
      'subgraph Cold["Cold path"]',
      'FreshMerge ~~~ Notes',
    ],
    excludes: [
      'Cold ~~~ Fresh',
      'Fresh ~~~ Research',
    ],
  },
  {
    name: 'Mermaid diagram component renders and preserves source',
    file: 'src/components/MermaidDiagram.astro',
    includes: [
      "import mermaid from 'mermaid';",
      'data-mermaid-diagram',
      'mermaid.render',
      'sourceLabel',
    ],
  },
  {
    name: 'storage-engine Mermaid diagrams use compact presentation',
    file: 'src/content/docs/proposals-research/storage-engine.mdx',
    includes: ['size="compact"'],
  },
  {
    name: 'memtable freshness topic page exists with compact Mermaid diagrams',
    file: 'src/content/docs/proposals-research/memtable-freshness.mdx',
    includes: [
      'title: Memtable Freshness',
      'export const freshnessMermaid',
      '<MermaidDiagram',
      'size="compact"',
      'docs/storage engine/report-1-memtable-freshness.md',
      'docs/storage engine/memtable-plugin-design.md',
    ],
  },
  {
    name: 'iceberg materializer topic page exists with compact Mermaid diagrams',
    file: 'src/content/docs/proposals-research/iceberg-materializer.mdx',
    includes: [
      'title: Lakehouse Materialization',
      'export const materializerMermaid',
      '<MermaidDiagram',
      'size="compact"',
      'docs/storage engine/proposal.md',
      'docs/storage engine/iceberg-oq1-build-vs-adopt.md',
    ],
  },
  {
    name: 'cassandra seam inventory topic page exists with compact Mermaid diagrams',
    file: 'src/content/docs/proposals-research/cassandra-seams.mdx',
    includes: [
      'title: Cassandra Seam Inventory',
      'export const seamsMermaid',
      '<MermaidDiagram',
      'size="compact"',
      'docs/storage engine/cassandra-index/write-path.md',
      'docs/storage engine/cassandra-index/read-path.md',
    ],
  },
  {
    name: 'read path and query providers page covers documented enhancement issues',
    file: 'src/content/docs/proposals-research/read-path-query-providers.mdx',
    includes: [
      'title: Read Path & Query Providers',
      'export const queryProviderMermaid',
      '<MermaidDiagram',
      'size="compact"',
      '#941',
      '#942',
      '#1336',
      'DataFusion',
      'docs/architecture/issue-941-datafusion-table-provider-council.md',
      'docs/942-point-read-fast-path-design.md',
      'docs/flight-trino/PLAN.md',
    ],
    excludes: [
      '#1045',
      'Spark connector',
    ],
  },
  {
    name: 'compaction maintenance page covers documented enhancement issues',
    file: 'src/content/docs/proposals-research/compaction-maintenance.mdx',
    includes: [
      'title: Compaction & Maintenance',
      'export const compactionMermaid',
      '<MermaidDiagram',
      'size="compact"',
      '#905',
      '#1536',
      'docs/compaction-manager-design.md',
      'docs/plans/2026-06-18-compaction-parity-harness-design.md',
    ],
  },
  {
    name: 'issue-only enhancement pages are not published as proposal topics',
    file: 'src/content/docs/proposals-research/performance-research.mdx',
    absent: true,
  },
  {
    name: 'broad API cleanup queue is not published as a proposal topic',
    file: 'src/content/docs/proposals-research/api-cli-maintenance-contracts.mdx',
    absent: true,
  },
  {
    name: 'compact Mermaid diagrams stay inside the article column',
    file: 'src/components/MermaidDiagram.astro',
    includes: [
      "size?: 'default' | 'compact';",
      'data-size={size}',
      '.mermaid-figure[data-size="compact"]',
      '.mermaid-figure:not([data-size="compact"])',
    ],
  },
  {
    name: 'generated concept image is present',
    file: 'src/assets/storage-engine-parallel-planes.png',
    binary: true,
  },
  {
    name: 'static architecture SVG was removed',
    file: 'src/assets/storage-engine-architecture.svg',
    absent: true,
  },
  {
    name: 'static data-path SVG was removed',
    file: 'src/assets/storage-engine-data-paths.svg',
    absent: true,
  },
  {
    name: 'architecture Mermaid separates OLTP writes from OLAP read path',
    file: 'src/content/docs/proposals-research/storage-engine.mdx',
    includes: [
      'Cassandra OLTP Plane',
      'Trino / Iceberg OLAP Plane',
      'Application reads/writes',
      'CommitLog',
      'Memtable',
      'Flush, compaction, repair',
      'SSTable Foundation',
      'Application analytical reads',
      'Trino',
      'Arrow Flight',
      'Iceberg materializer',
      'CQLite SSTable reader',
      'snapshot read',
      'optional fresh tail export',
    ],
    ordered: [
      'Application analytical reads',
      'Trino',
      'Arrow Flight',
      'Iceberg materializer',
      'CQLite SSTable reader',
    ],
    excludes: [
      'Operational writes, reads, repair, and lifecycle stay in Cassandra.',
      'CQLite reads Cassandra files for analytics without owning OLTP.',
      'src={architectureDiagram.src}',
    ],
  },
];

let failed = false;

for (const check of checks) {
  const path = join(root, check.file);
  if (check.absent) {
    if (existsSync(path)) {
      console.error(`FAIL ${check.name}: ${check.file} should not exist`);
      failed = true;
    } else {
      console.log(`PASS ${check.name}`);
    }
    continue;
  }

  if (!existsSync(path)) {
    console.error(`FAIL ${check.name}: missing ${check.file}`);
    failed = true;
    continue;
  }

  if (check.binary) {
    console.log(`PASS ${check.name}`);
    continue;
  }

  const contents = readFileSync(path, 'utf8');
  let checkFailed = false;

  for (const expected of check.includes) {
    if (!contents.includes(expected)) {
      console.error(
        `FAIL ${check.name}: ${check.file} does not include ${JSON.stringify(expected)}`
      );
      failed = true;
      checkFailed = true;
    }
  }

  if (check.excludes) {
    for (const unexpected of check.excludes) {
      if (contents.includes(unexpected)) {
        console.error(
          `FAIL ${check.name}: ${check.file} still includes ${JSON.stringify(unexpected)}`
        );
        failed = true;
        checkFailed = true;
      }
    }
  }

  if (check.ordered) {
    let cursor = -1;

    for (const expected of check.ordered) {
      const nextIndex = contents.indexOf(expected, cursor + 1);
      if (nextIndex === -1) {
        console.error(
          `FAIL ${check.name}: ${check.file} does not include ${JSON.stringify(expected)} after the previous ordered label`
        );
        failed = true;
        checkFailed = true;
        break;
      }
      cursor = nextIndex;
    }
  }

  if (!checkFailed) {
    console.log(`PASS ${check.name}`);
  }
}

if (failed) {
  process.exit(1);
}
