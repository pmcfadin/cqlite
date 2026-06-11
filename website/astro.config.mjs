// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

// https://astro.build/config
export default defineConfig({
  // Site URL for GitHub Pages project hosting
  site: 'https://pmcfadin.github.io',
  base: '/cqlite',

  integrations: [
    starlight({
      title: 'CQLite',
      description: 'Local Apache Cassandra SSTable access without cluster dependencies',
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
      ],
      sidebar: [
        {
          label: 'User Docs',
          autogenerate: { directory: 'user-docs' },
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
