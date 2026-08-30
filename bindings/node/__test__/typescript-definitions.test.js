/**
 * TypeScript Definitions Tests (Issue #312)
 *
 * Validates that TypeScript definitions are complete, correct, and usable.
 * These tests ensure:
 * 1. No 'any' types in public definitions
 * 2. All expected exports are present
 * 3. Type definitions match runtime implementation
 */

const fs = require('fs');
const path = require('path');

const LIB_DTS_PATH = path.join(__dirname, '..', 'lib', 'index.d.ts');

/**
 * Extract the members of a string-literal union type from `.d.ts` source.
 *
 * Deliberately NOT a "read to the first semicolon" parse: union members carry
 * trailing `//` comments and at least one of those comments itself contains a
 * `;` (the `TIMEOUT` member's "never 'IO';"). Slicing at the first raw `;`
 * truncates the union and silently UNDER-reads it — which would make a
 * containment assert pass vacuously in the dangerous direction. So comments are
 * stripped line-by-line FIRST, and only then is the declaration sliced at its
 * terminating semicolon.
 *
 * @param {string} dts - Full `.d.ts` source text
 * @param {string} typeName - e.g. 'ErrorCode'
 * @returns {string[]} Sorted, deduplicated union members
 */
function parseStringUnion(dts, typeName) {
  const codeOnly = dts
    .split('\n')
    .map((line) => {
      const commentAt = line.indexOf('//');
      return commentAt === -1 ? line : line.slice(0, commentAt);
    })
    .join('\n');

  const declaration = new RegExp(`export\\s+type\\s+${typeName}\\s*=`);
  const start = codeOnly.search(declaration);
  if (start === -1) {
    throw new Error(`type ${typeName} not found in index.d.ts`);
  }
  const end = codeOnly.indexOf(';', start);
  if (end === -1) {
    throw new Error(`type ${typeName} has no terminating ';' in index.d.ts`);
  }
  const body = codeOnly.slice(start, end);
  const members = (body.match(/'[^']+'/g) || []).map((m) => m.slice(1, -1));
  return [...new Set(members)].sort();
}

describe('TypeScript Definitions (Issue #312)', () => {
  let dtsContent;

  beforeAll(() => {
    dtsContent = fs.readFileSync(LIB_DTS_PATH, 'utf8');
  });

  describe('No "any" types', () => {
    test('should not contain ": any" type annotations', () => {
      // Match ": any" but not "any" within strings or comments
      const anyMatches = dtsContent.match(/:\s*any\b/g);
      expect(anyMatches).toBeNull();
    });

    test('should not contain "<any>" generic parameters', () => {
      const genericAnyMatches = dtsContent.match(/<\s*any\s*>/g);
      expect(genericAnyMatches).toBeNull();
    });

    test('should not contain "Array<any>" types', () => {
      const arrayAnyMatches = dtsContent.match(/Array\s*<\s*any\s*>/g);
      expect(arrayAnyMatches).toBeNull();
    });
  });

  describe('Required type exports', () => {
    test('should export Duration interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+Duration\b/);
    });

    test('should export UdtValue interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+UdtValue\b/);
    });

    test('should export Value type', () => {
      expect(dtsContent).toMatch(/export\s+type\s+Value\b/);
    });

    test('should export Row interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+Row\b/);
    });

    test('should export QueryResult interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+QueryResult\b/);
    });

    test('should export NativeQueryResult interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+NativeQueryResult\b/);
    });

    test('should export ColumnInfo interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+ColumnInfo\b/);
    });

    test('should export DatabaseStats interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+DatabaseStats\b/);
    });

    test('should export RefreshReport interface (issue #1749)', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+RefreshReport\b/);
    });

    test('should export DatabaseOptions interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+DatabaseOptions\b/);
    });

    test('should export StreamingConfig interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+StreamingConfig\b/);
    });

    test('should export ErrorCode type', () => {
      expect(dtsContent).toMatch(/export\s+type\s+ErrorCode\b/);
    });

    test('should export ErrorCategory type', () => {
      expect(dtsContent).toMatch(/export\s+type\s+ErrorCategory\b/);
    });

    test('should export CqliteError interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+CqliteError\b/);
    });

    test('should export Database class', () => {
      expect(dtsContent).toMatch(/export\s+declare\s+class\s+Database\b/);
    });

    test('should export version function', () => {
      expect(dtsContent).toMatch(/export\s+declare\s+function\s+version\b/);
    });

    test('should export PreparedStatement class', () => {
      expect(dtsContent).toMatch(/export\s+declare\s+class\s+PreparedStatement\b/);
    });

    test('should export PreparedStatementStats interface', () => {
      expect(dtsContent).toMatch(/export\s+interface\s+PreparedStatementStats\b/);
    });
  });

  describe('Database class methods', () => {
    test('should have static open method', () => {
      expect(dtsContent).toMatch(/static\s+open\s*\(/);
    });

    test('should have execute method', () => {
      expect(dtsContent).toMatch(/execute\s*\(\s*query\s*:\s*string\s*\)\s*:\s*Promise\s*<\s*QueryResult\s*>/);
    });

    test('should have executeNative method', () => {
      expect(dtsContent).toMatch(/executeNative\s*\(\s*query\s*:\s*string\s*\)\s*:\s*Promise\s*<\s*NativeQueryResult\s*>/);
    });

    test('should have getStats method', () => {
      expect(dtsContent).toMatch(/getStats\s*\(\s*\)\s*:\s*Promise\s*<\s*DatabaseStats\s*>/);
    });

    test('should have refresh method (issue #1749)', () => {
      expect(dtsContent).toMatch(/refresh\s*\(\s*\)\s*:\s*Promise\s*<\s*RefreshReport\s*>/);
    });

    test('should have close method', () => {
      expect(dtsContent).toMatch(/close\s*\(\s*\)\s*:\s*Promise\s*<\s*void\s*>/);
    });

    test('should have isClosed getter', () => {
      expect(dtsContent).toMatch(/get\s+isClosed\s*\(\s*\)\s*:\s*boolean/);
    });
  });

  describe('Value type completeness', () => {
    test('should include null in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*null\b/);
    });

    test('should include boolean in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*boolean\b/);
    });

    test('should include number in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*number\b/);
    });

    test('should include bigint in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*bigint\b/);
    });

    test('should include string in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*string\b/);
    });

    test('should include Buffer in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*Buffer\b/);
    });

    test('should include Date in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*Date\b/);
    });

    test('should include Duration in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*Duration\b/);
    });

    test('should include Value[] in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*Value\s*\[\s*\]/);
    });

    test('should include Set<Value> in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*Set\s*<\s*Value\s*>/);
    });

    test('should include Map<Value, Value> in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*Map\s*<\s*Value\s*,\s*Value\s*>/);
    });

    test('should include UdtValue in Value union', () => {
      expect(dtsContent).toMatch(/type\s+Value\s*=[\s\S]*?\|\s*UdtValue\b/);
    });
  });

  describe('Duration interface', () => {
    test('should have months property of type number', () => {
      expect(dtsContent).toMatch(/interface\s+Duration[\s\S]*?months\s*:\s*number/);
    });

    test('should have days property of type number', () => {
      expect(dtsContent).toMatch(/interface\s+Duration[\s\S]*?days\s*:\s*number/);
    });

    test('should have nanos property of type bigint', () => {
      expect(dtsContent).toMatch(/interface\s+Duration[\s\S]*?nanos\s*:\s*bigint/);
    });
  });

  describe('UdtValue interface', () => {
    test('should have _type property', () => {
      expect(dtsContent).toMatch(/interface\s+UdtValue[\s\S]*?_type\s*:\s*string/);
    });

    test('should have _keyspace property', () => {
      expect(dtsContent).toMatch(/interface\s+UdtValue[\s\S]*?_keyspace\s*:\s*string/);
    });

    test('should have index signature for fields', () => {
      expect(dtsContent).toMatch(/interface\s+UdtValue[\s\S]*?\[field\s*:\s*string\]\s*:\s*Value/);
    });
  });

  describe('Error types', () => {
    test('should include IO error code', () => {
      expect(dtsContent).toMatch(/type\s+ErrorCode[\s\S]*?\|\s*['"]IO['"]/);
    });

    test('should include SCHEMA error code', () => {
      expect(dtsContent).toMatch(/type\s+ErrorCode[\s\S]*?\|\s*['"]SCHEMA['"]/);
    });

    test('should include QUERY error code', () => {
      expect(dtsContent).toMatch(/type\s+ErrorCode[\s\S]*?\|\s*['"]QUERY['"]/);
    });

    test('should include PARSE error code', () => {
      expect(dtsContent).toMatch(/type\s+ErrorCode[\s\S]*?\|\s*['"]PARSE['"]/);
    });

    test('should include CANCELLED error code (issue #2264)', () => {
      expect(dtsContent).toMatch(/type\s+ErrorCode[\s\S]*?\|\s*['"]CANCELLED['"]/);
    });

    test('should include Cancelled error category (issue #2264)', () => {
      expect(dtsContent).toMatch(/type\s+ErrorCategory[\s\S]*?\|\s*['"]Cancelled['"]/);
    });

    /**
     * Issue #1451: the `ErrorCode` union and the shared FFI error contract's
     * `node_code` column are the SAME FACT WRITTEN TWICE, in two languages,
     * maintained by hand. A new core `Error` variant is forced to get a contract
     * row (`variant_of` is exhaustive, so it fails to compile) — but nothing
     * forces this union to gain the row's code, so a new code could ship while
     * TypeScript consumers are told it cannot occur. These two cases close that
     * in BOTH directions, taking the authoritative set from the TABLE (via the
     * `_errorContractNodeCodes` seam) rather than from a hand-written list here,
     * which would merely be a third copy of the same fact.
     */
    describe('ErrorCode union is pinned to the shared contract table (issue #1451)', () => {
      let tableCodes;
      let unionCodes;

      beforeAll(() => {
        // Required HERE, not at module scope: only these three cases need the
        // native addon, and a module-scope require would take the file's ~80
        // pure-text assertions down with it when the addon is not built. If it
        // IS missing these cases fail loudly (never skip) — the assert cannot be
        // performed without the authoritative table.
        const { _errorContractNodeCodes } = require('../lib/index.js');
        tableCodes = [...new Set(_errorContractNodeCodes())].sort();
        unionCodes = parseStringUnion(dtsContent, 'ErrorCode');
      });

      test('the parse actually read the union (never a vacuous pass)', () => {
        // A truncated/empty parse would make the containment assertions below
        // pass while checking nothing, so the parse is asserted first.
        expect(Array.isArray(tableCodes)).toBe(true);
        expect(tableCodes.length).toBeGreaterThan(10);
        expect(unionCodes.length).toBeGreaterThanOrEqual(tableCodes.length);
        // Members declared AFTER the `;`-bearing TIMEOUT comment: their presence
        // proves the parse did not truncate there.
        expect(unionCodes).toContain('IO');
        expect(unionCodes).toContain('MEMORY');
        expect(unionCodes).toContain('CANCELLED');
      });

      test('every code the contract can emit is declared in the union', () => {
        const missing = tableCodes.filter((code) => !unionCodes.includes(code));
        expect(missing).toEqual([]);
      });

      test('the union declares no code the contract never emits', () => {
        // `simple_error()` reuses INVALID_INPUT for non-core failures (e.g.
        // "Database is closed"), which the table also emits, so the two sets are
        // exactly equal today.
        const extra = unionCodes.filter((code) => !tableCodes.includes(code));
        expect(extra).toEqual([]);
      });
    });

    test('CqliteError should extend Error', () => {
      expect(dtsContent).toMatch(/interface\s+CqliteError\s+extends\s+Error\b/);
    });

    test('CqliteError should have code property', () => {
      expect(dtsContent).toMatch(/interface\s+CqliteError[\s\S]*?code\s*:\s*ErrorCode/);
    });

    test('CqliteError should have category property', () => {
      expect(dtsContent).toMatch(/interface\s+CqliteError[\s\S]*?category\s*:\s*ErrorCategory/);
    });

    test('CqliteError should have isRecoverable property', () => {
      expect(dtsContent).toMatch(/interface\s+CqliteError[\s\S]*?isRecoverable\s*:\s*boolean/);
    });
  });

  describe('JSDoc documentation', () => {
    test('should have JSDoc on Database class', () => {
      // Check for JSDoc comment before Database class
      expect(dtsContent).toMatch(/\/\*\*[\s\S]*?A CQLite database handle[\s\S]*?\*\/\s*export\s+declare\s+class\s+Database/);
    });

    test('should have JSDoc on Value type', () => {
      expect(dtsContent).toMatch(/\/\*\*[\s\S]*?All possible JavaScript values[\s\S]*?\*\/\s*export\s+type\s+Value/);
    });

    test('should have @example tags', () => {
      // Count @example occurrences - should have multiple
      const exampleMatches = dtsContent.match(/@example/g);
      expect(exampleMatches).not.toBeNull();
      expect(exampleMatches.length).toBeGreaterThanOrEqual(5);
    });

    test('should have @param tags for method parameters', () => {
      const paramMatches = dtsContent.match(/@param/g);
      expect(paramMatches).not.toBeNull();
      expect(paramMatches.length).toBeGreaterThanOrEqual(3);
    });

    test('should have @returns tags for methods', () => {
      const returnsMatches = dtsContent.match(/@returns/g);
      expect(returnsMatches).not.toBeNull();
      expect(returnsMatches.length).toBeGreaterThanOrEqual(3);
    });

    test('should have @throws tags for error-throwing methods', () => {
      const throwsMatches = dtsContent.match(/@throws/g);
      expect(throwsMatches).not.toBeNull();
      expect(throwsMatches.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('ColumnInfo interface', () => {
    test('should have name property', () => {
      expect(dtsContent).toMatch(/interface\s+ColumnInfo[\s\S]*?name\s*:\s*string/);
    });

    test('should have dataType property', () => {
      expect(dtsContent).toMatch(/interface\s+ColumnInfo[\s\S]*?dataType\s*:\s*string/);
    });

    test('should have nullable property', () => {
      expect(dtsContent).toMatch(/interface\s+ColumnInfo[\s\S]*?nullable\s*:\s*boolean/);
    });

    test('should have position property', () => {
      expect(dtsContent).toMatch(/interface\s+ColumnInfo[\s\S]*?position\s*:\s*number/);
    });

    test('should have tableName property', () => {
      expect(dtsContent).toMatch(/interface\s+ColumnInfo[\s\S]*?tableName\s*:\s*string\s*\|\s*null/);
    });
  });

  describe('QueryResult interfaces', () => {
    test('QueryResult should have rows property', () => {
      expect(dtsContent).toMatch(/interface\s+QueryResult[\s\S]*?rows\s*:\s*Record\s*<\s*string\s*,\s*unknown\s*>\s*\[\s*\]/);
    });

    test('QueryResult should have rowCount property', () => {
      expect(dtsContent).toMatch(/interface\s+QueryResult[\s\S]*?rowCount\s*:\s*number/);
    });

    test('QueryResult should have rowsAffected alias (Issue #348)', () => {
      expect(dtsContent).toMatch(/interface\s+QueryResult[\s\S]*?rowsAffected\s*:\s*number/);
    });

    test('QueryResult should have executionTimeMs property', () => {
      expect(dtsContent).toMatch(/interface\s+QueryResult[\s\S]*?executionTimeMs\s*:\s*number/);
    });

    test('QueryResult should have columns property', () => {
      expect(dtsContent).toMatch(/interface\s+QueryResult[\s\S]*?columns\s*:\s*ColumnInfo\s*\[\s*\]/);
    });

    test('NativeQueryResult should have rows of type Row[]', () => {
      expect(dtsContent).toMatch(/interface\s+NativeQueryResult[\s\S]*?rows\s*:\s*Row\s*\[\s*\]/);
    });

    test('NativeQueryResult should have rowsAffected alias (Issue #348)', () => {
      expect(dtsContent).toMatch(/interface\s+NativeQueryResult[\s\S]*?rowsAffected\s*:\s*number/);
    });
  });

  describe('Row interface', () => {
    test('should have index signature with Value type', () => {
      expect(dtsContent).toMatch(/interface\s+Row[\s\S]*?\[column\s*:\s*string\]\s*:\s*Value/);
    });
  });

  describe('DatabaseOptions interface (Issue #339)', () => {
    test('should have schema property of optional string', () => {
      expect(dtsContent).toMatch(/interface\s+DatabaseOptions[\s\S]*?schema\s*\?\s*:\s*string/);
    });

    test('should have memoryLimit property of optional number', () => {
      expect(dtsContent).toMatch(/interface\s+DatabaseOptions[\s\S]*?memoryLimit\s*\?\s*:\s*number/);
    });

    test('should have cacheEnabled property of optional boolean', () => {
      expect(dtsContent).toMatch(/interface\s+DatabaseOptions[\s\S]*?cacheEnabled\s*\?\s*:\s*boolean/);
    });

    test('memoryLimit should have JSDoc with default value', () => {
      // Verify documentation mentions the default (1GB)
      expect(dtsContent).toMatch(/memoryLimit[\s\S]*?Default:\s*1GB/);
    });

    test('cacheEnabled should have JSDoc with default value', () => {
      // Verify documentation mentions the default (true)
      expect(dtsContent).toMatch(/cacheEnabled[\s\S]*?Default:\s*true/);
    });
  });

  describe('Package configuration', () => {
    test('package.json should point types to lib/index.d.ts', () => {
      const packageJson = JSON.parse(
        fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8')
      );
      expect(packageJson.types).toBe('lib/index.d.ts');
    });
  });
});

/**
 * Runtime-surface drift alarm (issue #1456).
 *
 * The regex assertions above check that individual members are DECLARED, which
 * can only ever catch the members somebody thought to write a test for. This
 * block compares the two surfaces as SETS, so both drift directions fail:
 *
 *   - a `Database` prototype/static method missing from `index.d.ts`
 *     (invisible to every TypeScript caller), and
 *   - a member declared in `index.d.ts` with no runtime counterpart
 *     (a phantom declaration that type-checks and then throws).
 *
 * The declared side is read with the TypeScript compiler API rather than by
 * regex: `ts.createSourceFile` gives the real class-member list, so a member
 * inside a JSDoc block, a string literal or a commented-out line cannot be
 * mistaken for a declaration (and vice versa).
 */

const ts = require('typescript');

// Members every JS function object carries; they are not part of any declared
// class surface, so they are excluded from the static-side comparison.
const FUNCTION_INTRINSICS = new Set(['length', 'name', 'prototype']);

/**
 * Collect the members a `.d.ts` class declaration declares, split by staticness.
 *
 * Methods, properties and get/set accessors all count: each is an attribute a
 * caller can reach, which is exactly what the runtime comparison sees.
 *
 * @param {string} dtsSource - Full `.d.ts` source text
 * @param {string} className - e.g. 'Database'
 * @returns {{instance: string[], static: string[]}} Sorted member names
 */
function declaredClassMembers(dtsSource, className) {
  const sourceFile = ts.createSourceFile(
    'index.d.ts',
    dtsSource,
    ts.ScriptTarget.Latest,
    /* setParentNodes */ true
  );

  let declaration = null;
  const visit = (node) => {
    if (ts.isClassDeclaration(node) && node.name && node.name.text === className) {
      declaration = node;
      return;
    }
    ts.forEachChild(node, visit);
  };
  visit(sourceFile);

  if (declaration === null) {
    throw new Error(`class ${className} not found in index.d.ts`);
  }

  const instance = new Set();
  const statics = new Set();
  for (const member of declaration.members) {
    const isMember =
      ts.isMethodDeclaration(member) ||
      ts.isMethodSignature(member) ||
      ts.isPropertyDeclaration(member) ||
      ts.isPropertySignature(member) ||
      ts.isGetAccessorDeclaration(member) ||
      ts.isSetAccessorDeclaration(member);
    if (!isMember) {
      // Constructor signatures and index signatures are not named attributes.
      continue;
    }
    if (!member.name || !ts.isIdentifier(member.name)) {
      // Computed / string-literal member names have no runtime counterpart to
      // compare by name; there are none today, and skipping them silently would
      // be the permissive branch, so fail loudly instead.
      throw new Error(
        `class ${className} declares a member with a non-identifier name in index.d.ts`
      );
    }
    const isStatic = (ts.getCombinedModifierFlags(member) & ts.ModifierFlags.Static) !== 0;
    (isStatic ? statics : instance).add(member.name.text);
  }

  return {
    instance: [...instance].sort(),
    static: [...statics].sort(),
  };
}

/**
 * The members a runtime class actually exposes, split by staticness.
 *
 * @param {Function} cls - The runtime class (constructor function)
 * @returns {{instance: string[], static: string[]}} Sorted member names
 */
function runtimeClassMembers(cls) {
  const instance = Object.getOwnPropertyNames(cls.prototype)
    .filter((name) => name !== 'constructor')
    .sort();
  const statics = Object.getOwnPropertyNames(cls)
    .filter((name) => !FUNCTION_INTRINSICS.has(name))
    .sort();
  return { instance, static: statics };
}

/**
 * Compare the declared and runtime member sets, returning human-readable drift.
 *
 * @param {string} className - For the message
 * @param {{instance: string[], static: string[]}} declared
 * @param {{instance: string[], static: string[]}} runtime
 * @returns {string[]} One entry per drift direction found; empty when faithful
 */
function memberDrift(className, declared, runtime) {
  const drift = [];
  for (const kind of ['instance', 'static']) {
    const declaredSet = new Set(declared[kind]);
    const runtimeSet = new Set(runtime[kind]);
    const phantom = declared[kind].filter((name) => !runtimeSet.has(name));
    const undeclared = runtime[kind].filter((name) => !declaredSet.has(name));
    if (phantom.length > 0) {
      drift.push(
        `${className} ${kind}: declared in index.d.ts but absent at runtime ` +
          `(phantom declaration): ${phantom.join(', ')}`
      );
    }
    if (undeclared.length > 0) {
      drift.push(
        `${className} ${kind}: present at runtime but NOT declared in index.d.ts ` +
          `(invisible to TypeScript callers): ${undeclared.join(', ')}`
      );
    }
  }
  return drift;
}

/**
 * Every top-level NAME `index.d.ts` declares, whatever the declaration kind.
 *
 * A caller can write `import { X } from '@cqlite/node'` for a class, function,
 * interface, type alias, enum or exported const alike, so the runtime->declared
 * check must consider all of them -- not just classes and functions.
 *
 * @param {string} dtsSource - Full `.d.ts` source text
 * @returns {Set<string>} Declared top-level names
 */
function declaredTopLevelNames(dtsSource) {
  const sourceFile = ts.createSourceFile(
    'index.d.ts',
    dtsSource,
    ts.ScriptTarget.Latest,
    true
  );
  // Only EXPORTED, VALUE-BEARING, TOP-LEVEL declarations count as "declared" for
  // a runtime export, and each of those three qualifiers closes a false-PASS:
  //
  // * VALUE-BEARING -- an `interface` or `type` alias declares a TYPE and emits
  //   no value, so `QueryResult` being declared as an interface does NOT make a
  //   runtime `module.exports.QueryResult` usable by a TypeScript caller. Counting
  //   type-only declarations let a runtime value pass by NAME COLLISION with an
  //   unrelated interface.
  // * EXPORTED -- a declaration without `export` is not reachable by any caller.
  // * TOP-LEVEL -- the walk used to recurse with `forEachChild`, so a member or a
  //   declaration nested inside a namespace/module block satisfied a top-level
  //   export by name alone.
  //
  // This is the permissive-branch shape CLAUDE.md warns about: the test asked
  // "does this name appear anywhere in the .d.ts" when the property it needs is
  // "is this name an exported value declaration".
  const names = new Set();
  const isExported = (node) =>
    Boolean(
      node.modifiers &&
        node.modifiers.some((modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword)
    );

  for (const node of sourceFile.statements) {
    if (!isExported(node)) {
      continue;
    }
    // Classes, functions, enums and namespaces all emit a runtime value.
    if (
      (ts.isClassDeclaration(node) ||
        ts.isFunctionDeclaration(node) ||
        ts.isEnumDeclaration(node) ||
        ts.isModuleDeclaration(node)) &&
      node.name &&
      ts.isIdentifier(node.name)
    ) {
      names.add(node.name.text);
      continue;
    }
    // `export declare const x: T` also emits a value.
    if (ts.isVariableStatement(node)) {
      for (const declaration of node.declarationList.declarations) {
        if (ts.isIdentifier(declaration.name)) {
          names.add(declaration.name.text);
        }
      }
    }
    // Interfaces and type aliases are deliberately NOT collected: they are
    // type-only and cannot satisfy a runtime export.
  }
  return names;
}

describe('Runtime surface vs index.d.ts', () => {
  let dtsContent;
  let runtimeExports;

  beforeAll(() => {
    dtsContent = fs.readFileSync(LIB_DTS_PATH, 'utf8');
    // The published entry point (package.json `main`), i.e. the surface the
    // `.d.ts` describes -- NOT the raw napi binding, which `lib/index.js` wraps.
    runtimeExports = require('../lib/index.js');
  });

  test('Database declared members equal the runtime members', () => {
    const { Database } = runtimeExports;
    expect(typeof Database).toBe('function');

    const declared = declaredClassMembers(dtsContent, 'Database');
    const runtime = runtimeClassMembers(Database);

    // Sanity: an empty side would make the comparison vacuous in the dangerous
    // direction (an empty declared set "matches" nothing missing).
    expect(declared.instance.length).toBeGreaterThan(0);
    expect(declared.static).toContain('open');
    expect(runtime.instance.length).toBeGreaterThan(0);
    expect(runtime.static).toContain('open');

    expect(memberDrift('Database', declared, runtime)).toEqual([]);
  });

  test('PreparedStatement declared members equal the runtime members', () => {
    const { PreparedStatement } = runtimeExports;
    expect(typeof PreparedStatement).toBe('function');

    const declared = declaredClassMembers(dtsContent, 'PreparedStatement');
    const runtime = runtimeClassMembers(PreparedStatement);

    expect(declared.instance.length).toBeGreaterThan(0);
    expect(runtime.instance.length).toBeGreaterThan(0);

    expect(memberDrift('PreparedStatement', declared, runtime)).toEqual([]);
  });

  test('every declared class resolves to a runtime export', () => {
    // Phantom-class direction: `export declare class Foo` with nothing exported
    // under that name type-checks and then fails at `new Foo()`.
    const sourceFile = ts.createSourceFile(
      'index.d.ts',
      dtsContent,
      ts.ScriptTarget.Latest,
      true
    );
    const declaredClasses = [];
    const visit = (node) => {
      if (ts.isClassDeclaration(node) && node.name) {
        declaredClasses.push(node.name.text);
      }
      ts.forEachChild(node, visit);
    };
    visit(sourceFile);

    expect(declaredClasses.length).toBeGreaterThan(0);
    const phantoms = declaredClasses.filter(
      (name) => typeof runtimeExports[name] !== 'function'
    );
    expect(phantoms).toEqual([]);
  });

  test('every declared function resolves to a runtime export', () => {
    const sourceFile = ts.createSourceFile(
      'index.d.ts',
      dtsContent,
      ts.ScriptTarget.Latest,
      true
    );
    const declaredFunctions = [];
    const visit = (node) => {
      if (ts.isFunctionDeclaration(node) && node.name) {
        declaredFunctions.push(node.name.text);
      }
      ts.forEachChild(node, visit);
    };
    visit(sourceFile);

    expect(declaredFunctions.length).toBeGreaterThan(0);
    const phantoms = declaredFunctions.filter(
      (name) => typeof runtimeExports[name] !== 'function'
    );
    expect(phantoms).toEqual([]);
  });

  test('every public runtime export is declared in index.d.ts', () => {
    // The declared->runtime direction is covered above. This is the OTHER
    // direction, and it is the scenario issue #1456 exists for: a new PUBLIC
    // export added to `lib/index.js` and forgotten in `index.d.ts` is invisible
    // to every TypeScript caller, and nothing else in this suite notices.
    //
    // Underscore-prefixed exports are excluded because they are internal test
    // hooks, not API: `_errorContractProbe` and `_errorContractNodeCodes`
    // (issue #1451) and `_ffiCommonRenderVectors` (issue #1452) are reached only
    // by this test suite, and each is documented `@private` in `lib/index.js`.
    // This mirrors the Python side, which scopes its `__all__`-vs-stub direction
    // to non-underscore names for the same reason.
    const publicExports = Object.keys(runtimeExports)
      .filter((name) => !name.startsWith('_'))
      .sort();
    // Non-vacuity: an empty public-export set would satisfy the assert below
    // trivially, so an entry point that failed to load could green.
    expect(publicExports.length).toBeGreaterThan(0);
    expect(publicExports).toContain('Database');

    const declared = declaredTopLevelNames(dtsContent);
    expect(declared.size).toBeGreaterThan(0);

    const undeclared = publicExports.filter((name) => !declared.has(name));
    expect(undeclared).toEqual([]);
  });
});
