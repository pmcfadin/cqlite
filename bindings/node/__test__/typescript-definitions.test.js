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
const { _errorContractNodeCodes } = require('../lib/index.js');

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
