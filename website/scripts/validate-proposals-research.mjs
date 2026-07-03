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
    name: 'generated concept image is present',
    file: 'src/assets/storage-engine-parallel-planes.png',
    binary: true,
  },
];

let failed = false;

for (const check of checks) {
  const path = join(root, check.file);
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

  if (!checkFailed) {
    console.log(`PASS ${check.name}`);
  }
}

if (failed) {
  process.exit(1);
}
