// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';
import starlightLlmsTxt from 'starlight-llms-txt';

// https://astro.build/config
export default defineConfig({
  // Site URL for GitHub Pages project hosting
  site: 'https://pmcfadin.github.io',
  base: '/cqlite',

  integrations: [
    starlight({
      title: 'CQLite',
      description: 'Local Apache Cassandra SSTable access without cluster dependencies',
      logo: {
        src: './src/assets/cqlite.png',
        alt: 'CQLite',
        replacesTitle: true,
      },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/pmcfadin/cqlite',
        },
      ],
      editLink: {
        baseUrl: 'https://github.com/pmcfadin/cqlite/edit/main/website/',
      },
      plugins: [
        starlightLinksValidator({
          // Fail the build on internal link/anchor breakage.
          // Exclude /cqlite/api/** paths: these are rustdoc pages published by
          // api-docs.yml to a separate subtree of gh-pages and are not part of
          // the Starlight site build.
          exclude: ['/cqlite/api/**'],
        }),
        starlightLlmsTxt({
          // Generate llms.txt (section map + one-line descriptions) and
          // llms-full.txt (full content of every page) at the site root.
          // Plugin version: 0.6.1 (pinned in package.json).
          // URLs in the generated files use the site + base from astro.config.mjs,
          // so absolute URLs are: https://pmcfadin.github.io/cqlite/<page-slug>/
          description: 'Local Apache Cassandra SSTable access without cluster dependencies. Reads Cassandra 5.0 SSTables directly from disk — no cluster, no JVM required.',
        }),
      ],
      sidebar: [
        {
          label: 'User Docs',
          autogenerate: { directory: 'user-docs' },
        },
        {
          label: 'Proposals and Research',
          autogenerate: { directory: 'proposals-research' },
        },
        {
          label: 'Releases',
          autogenerate: { directory: 'releases' },
        },
        {
          label: 'SSTable Format Guide',
          autogenerate: { directory: 'sstable-format' },
        },
        {
          label: 'For Agents: Using CQLite',
          autogenerate: { directory: 'agents-using' },
        },
        {
          label: 'For Agents: Developing CQLite',
          autogenerate: { directory: 'agents-developing' },
        },
      ],
    }),
  ],
});
