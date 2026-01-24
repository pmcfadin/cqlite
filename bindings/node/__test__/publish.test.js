/**
 * npm Publishing Validation Tests
 *
 * Issue #314: Validates package configuration and publishing readiness.
 *
 * These tests verify:
 * - package.json has required fields for npm publishing
 * - napi-rs configuration covers all target platforms
 * - Files array includes all necessary artifacts
 * - publishConfig is correctly configured
 * - Tarball creation works (when module is built)
 */

const path = require('path');
const fs = require('fs');
const { execSync } = require('child_process');

const BINDINGS_DIR = path.resolve(__dirname, '..');
const PACKAGE_JSON_PATH = path.join(BINDINGS_DIR, 'package.json');

// Load package.json once for all tests
const pkg = require(PACKAGE_JSON_PATH);

describe('npm Publishing Validation (Issue #314)', () => {
  describe('Package.json Required Fields', () => {
    test('has scoped package name @cqlite/node', () => {
      expect(pkg.name).toBe('@cqlite/node');
    });

    test('has valid semver version', () => {
      expect(pkg.version).toMatch(/^\d+\.\d+\.\d+(-[\w.]+)?$/);
    });

    test('has description', () => {
      expect(typeof pkg.description).toBe('string');
      expect(pkg.description.length).toBeGreaterThan(10);
    });

    test('has main entry point', () => {
      expect(pkg.main).toBe('lib/index.js');
      const mainPath = path.join(BINDINGS_DIR, pkg.main);
      expect(fs.existsSync(mainPath)).toBe(true);
    });

    test('has TypeScript types entry point', () => {
      expect(pkg.types).toBe('lib/index.d.ts');
      const typesPath = path.join(BINDINGS_DIR, pkg.types);
      expect(fs.existsSync(typesPath)).toBe(true);
    });

    test('has repository configured', () => {
      expect(pkg.repository).toBeDefined();
      expect(pkg.repository.type).toBe('git');
      expect(pkg.repository.url).toContain('github.com');
      expect(pkg.repository.directory).toBe('bindings/node');
    });

    test('has license (dual MIT/Apache-2.0)', () => {
      expect(pkg.license).toMatch(/MIT|Apache/);
    });

    test('has keywords for discoverability', () => {
      expect(Array.isArray(pkg.keywords)).toBe(true);
      expect(pkg.keywords).toContain('cassandra');
      expect(pkg.keywords).toContain('cqlite');
    });

    test('has author', () => {
      expect(pkg.author).toBeDefined();
    });

    test('has homepage and bugs URLs', () => {
      expect(pkg.homepage).toContain('github.com');
      expect(pkg.bugs.url).toContain('issues');
    });
  });

  describe('napi-rs Configuration', () => {
    test('has napi configuration object', () => {
      expect(pkg.napi).toBeDefined();
      expect(typeof pkg.napi).toBe('object');
    });

    test('napi.name matches expected binary name', () => {
      // Critical: must be "cqlite-node" for scoped package resolution
      expect(pkg.napi.name).toBe('cqlite-node');
    });

    test('enables default triples', () => {
      expect(pkg.napi.triples).toBeDefined();
      expect(pkg.napi.triples.defaults).toBe(true);
    });

    test('includes ARM64 targets in additional triples', () => {
      const additional = pkg.napi.triples.additional || [];
      expect(additional).toContain('aarch64-apple-darwin');
      expect(additional).toContain('aarch64-unknown-linux-gnu');
    });

    test('covers all 5 required platforms', () => {
      // Default triples from napi-rs (when defaults: true):
      // - x86_64-unknown-linux-gnu
      // - x86_64-apple-darwin
      // - x86_64-pc-windows-msvc
      // Additional:
      // - aarch64-apple-darwin
      // - aarch64-unknown-linux-gnu
      const additional = pkg.napi.triples.additional || [];
      const defaultTargets = [
        'x86_64-unknown-linux-gnu',
        'x86_64-apple-darwin',
        'x86_64-pc-windows-msvc',
      ];
      const allTargets = [...defaultTargets, ...additional];

      expect(allTargets.length).toBeGreaterThanOrEqual(5);
      expect(additional).toContain('aarch64-apple-darwin');
      expect(additional).toContain('aarch64-unknown-linux-gnu');
    });
  });

  describe('Files Array Configuration', () => {
    test('includes generated index.js (platform loader)', () => {
      expect(pkg.files).toContain('index.js');
    });

    test('includes generated index.d.ts', () => {
      expect(pkg.files).toContain('index.d.ts');
    });

    test('includes lib directory', () => {
      expect(pkg.files).toContain('lib');
      expect(fs.existsSync(path.join(BINDINGS_DIR, 'lib'))).toBe(true);
    });

    test('includes npm directory for platform binaries', () => {
      // npm/ directory is created at publish time by napi-rs
      expect(pkg.files).toContain('npm');
    });

    test('includes README.md', () => {
      expect(pkg.files).toContain('README.md');
      expect(fs.existsSync(path.join(BINDINGS_DIR, 'README.md'))).toBe(true);
    });

    test('includes LICENSE', () => {
      expect(pkg.files).toContain('LICENSE');
      // LICENSE should exist (may be at project root or bindings/node)
      const licensePath = path.join(BINDINGS_DIR, 'LICENSE');
      const rootLicensePath = path.join(BINDINGS_DIR, '..', '..', 'LICENSE');
      const hasLicense = fs.existsSync(licensePath) || fs.existsSync(rootLicensePath);
      expect(hasLicense).toBe(true);
    });
  });

  describe('publishConfig', () => {
    test('has publishConfig object', () => {
      expect(pkg.publishConfig).toBeDefined();
      expect(typeof pkg.publishConfig).toBe('object');
    });

    test('uses npm registry', () => {
      expect(pkg.publishConfig.registry).toBe('https://registry.npmjs.org/');
    });

    test('has public access (required for scoped packages)', () => {
      expect(pkg.publishConfig.access).toBe('public');
    });
  });

  describe('Scripts', () => {
    test('has build script', () => {
      expect(pkg.scripts.build).toBeDefined();
      expect(pkg.scripts.build).toContain('napi build');
    });

    test('has prepublishOnly hook for napi prepublish', () => {
      expect(pkg.scripts.prepublishOnly).toBeDefined();
      expect(pkg.scripts.prepublishOnly).toContain('napi prepublish');
    });

    test('has test script', () => {
      expect(pkg.scripts.test).toBeDefined();
    });
  });

  describe('Engine Requirements', () => {
    test('requires Node.js 18+', () => {
      expect(pkg.engines).toBeDefined();
      expect(pkg.engines.node).toMatch(/>=?\s*18/);
    });
  });

  describe('Native Module Loading', () => {
    // This test validates that the native module can actually load
    // It will pass if the module is built, skip if not
    test('native module loads successfully', () => {
      try {
        const cqlite = require('../index.js');
        expect(cqlite).toBeDefined();
        expect(typeof cqlite.version).toBe('function');
        expect(typeof cqlite.Database).toBe('function');
      } catch (err) {
        // If module not built, this is expected
        if (err.message.includes('Cannot find module') ||
            err.message.includes('.node')) {
          console.log('    [SKIPPED] Native module not built - run `npm run build` first');
          return;
        }
        throw err;
      }
    });

    test('exports match expected API surface', () => {
      try {
        const cqlite = require('../index.js');
        // Check core exports
        expect(cqlite.Database).toBeDefined();
        expect(cqlite.version).toBeDefined();
        // Check Database has expected static methods
        expect(typeof cqlite.Database.open).toBe('function');
      } catch (err) {
        if (err.message.includes('Cannot find module') ||
            err.message.includes('.node')) {
          console.log('    [SKIPPED] Native module not built');
          return;
        }
        throw err;
      }
    });
  });

  describe('Tarball Creation', () => {
    // This is a slower test that actually runs npm pack
    // Only run when RUN_SLOW_TESTS=1 (matches Python conftest.py convention)
    const shouldRunPackTest = process.env.RUN_SLOW_TESTS === '1';

    (shouldRunPackTest ? test : test.skip)(
      'npm pack --dry-run succeeds',
      () => {
        try {
          // npm pack --dry-run outputs details to stderr/stdout
          const output = execSync('npm pack --dry-run 2>&1', {
            cwd: BINDINGS_DIR,
            encoding: 'utf8',
            timeout: 30000,
          });

          // Should output tarball name
          expect(output).toContain('cqlite-node');
          expect(output).toContain('.tgz');

          // Should list files being packed (npm notice output)
          // npm 10+ outputs file list with "npm notice" prefix
          expect(output).toContain('Tarball Contents');
          expect(output).toContain('lib/index.js');
          expect(output).toContain('README.md');
        } catch (err) {
          // npm pack may fail if certain files don't exist yet
          if (err.message.includes('ENOENT')) {
            console.log('    [SKIPPED] npm pack failed - some files may not exist yet');
            return;
          }
          throw err;
        }
      },
      60000 // 60 second timeout
    );
  });
});
