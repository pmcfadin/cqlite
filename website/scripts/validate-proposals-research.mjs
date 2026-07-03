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
    name: 'proposals hub exists and features storage engine direction',
    file: 'src/content/docs/proposals-research/index.mdx',
    includes: [
      'title: Proposals and Research',
      'Storage Engine Direction',
      '/cqlite/proposals-research/storage-engine/',
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
