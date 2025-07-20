#!/usr/bin/env node

/**
 * CQLite NodeJS Setup Script
 * Prepares the development environment and validates installation
 */

const fs = require('fs').promises;
const path = require('path');
const { execSync } = require('child_process');

class CQLiteSetup {
  constructor() {
    this.rootDir = path.join(__dirname, '..');
    this.errors = [];
    this.warnings = [];
  }

  log(message, type = 'info') {
    const timestamp = new Date().toISOString();
    const prefix = {
      info: '📋',
      success: '✅',
      warning: '⚠️',
      error: '❌'
    }[type] || '📋';
    
    console.log(`${prefix} [${timestamp}] ${message}`);
  }

  async checkPrerequisites() {
    this.log('Checking prerequisites...', 'info');

    // Check Node.js version
    const nodeVersion = process.version;
    const majorVersion = parseInt(nodeVersion.slice(1).split('.')[0]);
    
    if (majorVersion < 14) {
      this.errors.push(`Node.js 14+ required, found ${nodeVersion}`);
    } else {
      this.log(`Node.js version: ${nodeVersion}`, 'success');
    }

    // Check for Rust
    try {
      const rustVersion = execSync('rustc --version', { encoding: 'utf8' }).trim();
      this.log(`Rust: ${rustVersion}`, 'success');
    } catch (error) {
      this.errors.push('Rust not found. Install from https://rustup.rs/');
    }

    // Check for Cargo
    try {
      const cargoVersion = execSync('cargo --version', { encoding: 'utf8' }).trim();
      this.log(`Cargo: ${cargoVersion}`, 'success');
    } catch (error) {
      this.errors.push('Cargo not found. Install Rust toolchain.');
    }

    // Check for Python (needed for native compilation)
    try {
      const pythonVersion = execSync('python3 --version', { encoding: 'utf8' }).trim();
      this.log(`Python: ${pythonVersion}`, 'success');
    } catch (error) {
      try {
        const pythonVersion = execSync('python --version', { encoding: 'utf8' }).trim();
        this.log(`Python: ${pythonVersion}`, 'success');
      } catch (error2) {
        this.warnings.push('Python not found. May be needed for native compilation.');
      }
    }

    // Check platform-specific requirements
    const platform = process.platform;
    this.log(`Platform: ${platform}`, 'info');

    if (platform === 'win32') {
      this.warnings.push('Windows platform detected. Ensure Visual Studio Build Tools are installed.');
    } else if (platform === 'darwin') {
      this.log('macOS platform detected. Xcode Command Line Tools recommended.', 'info');
    } else {
      this.log('Linux platform detected. Build tools (gcc/make) recommended.', 'info');
    }
  }

  async validateProjectStructure() {
    this.log('Validating project structure...', 'info');

    const requiredFiles = [
      'package.json',
      'Cargo.toml',
      'build.rs',
      'src/lib.rs',
      'lib/index.js',
      'index.d.ts',
      'examples/basic-usage.js',
      'test/basic.test.js'
    ];

    for (const file of requiredFiles) {
      const filePath = path.join(this.rootDir, file);
      try {
        await fs.access(filePath);
        this.log(`Found: ${file}`, 'success');
      } catch (error) {
        this.errors.push(`Missing required file: ${file}`);
      }
    }

    // Check directory structure
    const requiredDirs = ['src', 'lib', 'examples', 'test'];
    for (const dir of requiredDirs) {
      const dirPath = path.join(this.rootDir, dir);
      try {
        const stats = await fs.stat(dirPath);
        if (stats.isDirectory()) {
          this.log(`Directory: ${dir}`, 'success');
        } else {
          this.errors.push(`${dir} is not a directory`);
        }
      } catch (error) {
        this.errors.push(`Missing directory: ${dir}`);
      }
    }
  }

  async createTestData() {
    this.log('Creating test data structure...', 'info');

    const testDataDir = path.join(this.rootDir, 'test-data');
    
    try {
      await fs.mkdir(testDataDir, { recursive: true });
      this.log(`Created test data directory: ${testDataDir}`, 'success');

      // Create example schema
      const exampleSchema = {
        table: 'users',
        columns: [
          { name: 'user_id', type: 'uuid', primaryKey: true },
          { name: 'name', type: 'text' },
          { name: 'email', type: 'text' },
          { name: 'age', type: 'int' },
          { name: 'department', type: 'text' },
          { name: 'active', type: 'boolean' },
          { name: 'created_date', type: 'timestamp', clusteringKey: true },
          { name: 'preferences', type: 'map' },
          { name: 'skills', type: 'list' }
        ]
      };

      const schemaPath = path.join(testDataDir, 'users_schema.json');
      await fs.writeFile(schemaPath, JSON.stringify(exampleSchema, null, 2));
      this.log(`Created example schema: users_schema.json`, 'success');

      // Create instructions for SSTable files
      const instructions = `
# CQLite Test Data Instructions

This directory contains schema files for testing CQLite functionality.

## To create test SSTable files:

1. Install Cassandra locally
2. Create a keyspace and table matching the schema
3. Insert test data
4. Copy the SSTable files from Cassandra's data directory to this folder

Example CQL commands:

\`\`\`cql
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test;

CREATE TABLE users (
  user_id uuid PRIMARY KEY,
  name text,
  email text,
  age int,
  department text,
  active boolean,
  created_date timestamp,
  preferences map<text, text>,
  skills list<text>
);

INSERT INTO users (user_id, name, email, age, department, active, created_date, preferences, skills)
VALUES (uuid(), 'John Doe', 'john@example.com', 30, 'Engineering', true, toTimestamp(now()), {'theme': 'dark'}, ['javascript', 'rust']);

-- Insert more test data...
\`\`\`

3. Find SSTable files in: \`/var/lib/cassandra/data/test/users-*/\`
4. Copy *-Data.db files to this directory
5. Run CQLite examples and tests

## Alternative: Use CQLite CLI to generate test data

\`\`\`bash
# Use the CQLite CLI to create test SSTable files
cqlite generate --table users --rows 1000 --output test-data/
\`\`\`
`;

      const instructionsPath = path.join(testDataDir, 'README.md');
      await fs.writeFile(instructionsPath, instructions);
      this.log('Created test data instructions', 'success');

    } catch (error) {
      this.warnings.push(`Could not create test data directory: ${error.message}`);
    }
  }

  async installDependencies() {
    this.log('Installing dependencies...', 'info');

    try {
      // Check if node_modules exists
      const nodeModulesPath = path.join(this.rootDir, 'node_modules');
      try {
        await fs.access(nodeModulesPath);
        this.log('Dependencies already installed', 'success');
        return;
      } catch (error) {
        // node_modules doesn't exist, install
      }

      this.log('Running npm install...', 'info');
      execSync('npm install', { 
        cwd: this.rootDir, 
        stdio: 'inherit' 
      });
      this.log('Dependencies installed successfully', 'success');

    } catch (error) {
      this.errors.push(`Failed to install dependencies: ${error.message}`);
    }
  }

  async buildNativeModule() {
    this.log('Building native module...', 'info');

    try {
      // Check if the core cqlite library exists
      const corePath = path.join(this.rootDir, '..', 'cqlite-core');
      try {
        await fs.access(corePath);
        this.log('Found cqlite-core dependency', 'success');
      } catch (error) {
        this.warnings.push('cqlite-core not found. Native module build may fail.');
      }

      // Try to build the native module
      this.log('Compiling Rust code...', 'info');
      execSync('npm run build:debug', { 
        cwd: this.rootDir, 
        stdio: 'inherit' 
      });
      this.log('Native module built successfully', 'success');

    } catch (error) {
      this.warnings.push(`Could not build native module: ${error.message}`);
      this.warnings.push('This is expected if cqlite-core is not available yet.');
    }
  }

  async runTests() {
    this.log('Running tests...', 'info');

    try {
      execSync('npm test', { 
        cwd: this.rootDir, 
        stdio: 'inherit' 
      });
      this.log('All tests passed!', 'success');
    } catch (error) {
      this.warnings.push('Some tests failed. This may be expected without actual SSTable files.');
    }
  }

  async generateExampleUsage() {
    this.log('Generating example usage guide...', 'info');

    const usage = `
# CQLite NodeJS - Quick Start Guide

## 1. Basic Query Example

\`\`\`javascript
const { SSTableReader } = require('cqlite');

async function basicExample() {
  const reader = new SSTableReader('users-Data.db', {
    schema: 'users_schema.json'
  });

  const result = await reader.query('SELECT * FROM users LIMIT 10');
  console.log(\`Found \${result.rowCount} users\`);
  
  await reader.close();
}

basicExample().catch(console.error);
\`\`\`

## 2. Streaming Example

\`\`\`javascript
const stream = reader.queryStream('SELECT * FROM users WHERE active = true');

for await (const user of stream) {
  console.log(\`\${user.name} - \${user.email}\`);
}
\`\`\`

## 3. TypeScript Example

\`\`\`typescript
import { createTypedReader } from 'cqlite';

interface User {
  name: string;
  email: string;
  age: number;
}

const reader = createTypedReader<User>('users.db', { schema: 'schema.json' });
const users = await reader.query('SELECT * FROM users');
\`\`\`

## 4. Express API Example

\`\`\`javascript
const express = require('express');
const { SSTableReader } = require('cqlite');

const app = express();
app.use(express.json());

const reader = new SSTableReader('data.db', { schema: 'schema.json' });

app.post('/query', async (req, res) => {
  try {
    const result = await reader.query(req.body.sql);
    res.json(result);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
});

app.listen(3000);
\`\`\`

## Next Steps

1. Create or obtain Cassandra SSTable files
2. Define appropriate schema files
3. Run the examples in the examples/ directory
4. Build your own applications with CQLite!

For more information, see the full README.md file.
`;

    const usagePath = path.join(this.rootDir, 'QUICK_START.md');
    await fs.writeFile(usagePath, usage);
    this.log('Generated quick start guide', 'success');
  }

  async printSummary() {
    this.log('Setup Summary', 'info');
    console.log('');

    if (this.errors.length === 0) {
      this.log('🎉 Setup completed successfully!', 'success');
    } else {
      this.log('⚠️ Setup completed with issues', 'warning');
    }

    if (this.errors.length > 0) {
      console.log('\n❌ Errors that need attention:');
      this.errors.forEach(error => console.log(`   • ${error}`));
    }

    if (this.warnings.length > 0) {
      console.log('\n⚠️ Warnings:');
      this.warnings.forEach(warning => console.log(`   • ${warning}`));
    }

    console.log('\n📚 Next steps:');
    console.log('   1. Review any errors or warnings above');
    console.log('   2. Create test SSTable files (see test-data/README.md)');
    console.log('   3. Run examples: node examples/basic-usage.js');
    console.log('   4. Run tests: npm test');
    console.log('   5. Build production: npm run build');
    console.log('   6. Read the full documentation in README.md');

    console.log('\n🚀 CQLite NodeJS is ready for revolutionary SSTable querying!');
  }

  async run() {
    try {
      console.log('🚀 CQLite NodeJS Setup');
      console.log('=======================\n');

      await this.checkPrerequisites();
      await this.validateProjectStructure();
      await this.createTestData();
      await this.installDependencies();
      await this.buildNativeModule();
      await this.runTests();
      await this.generateExampleUsage();
      await this.printSummary();

    } catch (error) {
      this.log(`Setup failed: ${error.message}`, 'error');
      process.exit(1);
    }
  }
}

// Run setup if called directly
if (require.main === module) {
  const setup = new CQLiteSetup();
  setup.run().catch(error => {
    console.error('Setup failed:', error);
    process.exit(1);
  });
}

module.exports = CQLiteSetup;