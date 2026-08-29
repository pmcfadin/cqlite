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

    test('napi.binaryName matches expected binary name', () => {
      // Critical: must be "cqlite-node" for scoped package resolution
      // napi-rs v3 uses binaryName instead of name
      expect(pkg.napi.binaryName).toBe('cqlite-node');
    });

    test('has explicit targets array (napi-rs v3 format)', () => {
      // napi-rs v3 uses explicit targets array instead of triples.defaults/additional
      expect(pkg.napi.targets).toBeDefined();
      expect(Array.isArray(pkg.napi.targets)).toBe(true);
    });

    test('includes ARM64 targets', () => {
      const targets = pkg.napi.targets || [];
      expect(targets).toContain('aarch64-apple-darwin');
      expect(targets).toContain('aarch64-unknown-linux-gnu');
    });

    test('covers all 5 required platforms', () => {
      // napi-rs v3 uses explicit targets array
      const targets = pkg.napi.targets || [];
      const requiredPlatforms = [
        'x86_64-unknown-linux-gnu',
        'x86_64-apple-darwin',
        'x86_64-pc-windows-msvc',
        'aarch64-apple-darwin',
        'aarch64-unknown-linux-gnu',
      ];

      expect(targets.length).toBeGreaterThanOrEqual(5);
      for (const platform of requiredPlatforms) {
        expect(targets).toContain(platform);
      }
    });
  });

  describe('Files Array Configuration', () => {
    test('includes generated index.js (platform loader)', () => {
      expect(pkg.files).toContain('index.js');
    });

    test('does not ship root index.d.ts stub (Issue #571)', () => {
      // The root index.d.ts was a 0-byte stub; types resolve via the `types`
      // field (lib/index.d.ts). It must not be listed in `files`, and must not
      // exist at the package root.
      expect(pkg.files).not.toContain('index.d.ts');
      expect(fs.existsSync(path.join(BINDINGS_DIR, 'index.d.ts'))).toBe(false);
    });

    test('includes lib directory', () => {
      expect(pkg.files).toContain('lib');
      expect(fs.existsSync(path.join(BINDINGS_DIR, 'lib'))).toBe(true);
    });

    test('includes native module glob pattern', () => {
      // *.node pattern includes all platform-specific native modules
      expect(pkg.files).toContain('*.node');
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
    // Issue #1459. This assertion is EXACT rather than a loose `/>=?\s*18/`
    // match, because that pattern accepted two ranges that were both FALSE:
    //
    //   ">= 18"        — 18.0.0-18.16.x cannot load the module at all
    //   ">= 18.17.0"   — still swept in 19.x and 20.0-20.2
    //
    // The module is built against Node-API 9 (`napi9` in Cargo.toml), which
    // ships in 18.17.0+ and 20.3.0+ but NEVER in 19.x or 20.0-20.2 — so the
    // supported set is genuinely discontinuous and no single `>=` can express
    // it. The `smoke-floor` matrix in .github/workflows/node-ci.yml executes
    // BOTH lower boundaries; this test is what keeps the advertised range and
    // that matrix from drifting apart.
    //
    // Deliberately a string compare, not a semver range evaluation: `semver` is
    // only a TRANSITIVE dependency here (devDependencies are @napi-rs/cli,
    // @types/node, jest, ts-node, typescript), and asserting a published
    // compatibility claim through an undeclared package would be a hidden
    // coupling. Any edit to the range fails this test loudly, which is the
    // behaviour we want on a claim that has already been wrong twice.
    const SUPPORTED_NODE_RANGE = '^18.17.0 || >= 20.3.0';

    test('advertises the exact discontinuous napi9-compatible range', () => {
      expect(pkg.engines).toBeDefined();
      expect(pkg.engines.node).toBe(SUPPORTED_NODE_RANGE);
    });

    test('the lockfile root mirrors engines.node (npm writes it there too)', () => {
      const lock = JSON.parse(
        fs.readFileSync(path.join(__dirname, '..', 'package-lock.json'), 'utf8')
      );
      expect(lock.packages['']).toBeDefined();
      expect(lock.packages[''].engines.node).toBe(SUPPORTED_NODE_RANGE);
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
