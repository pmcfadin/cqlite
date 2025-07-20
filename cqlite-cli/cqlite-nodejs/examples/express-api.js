#!/usr/bin/env node

/**
 * CQLite Express API Example
 * Revolutionary REST API that queries SSTable files directly!
 */

const express = require('express');
const { SSTableReader, Utils, CQLiteError } = require('../lib/index.js');
const path = require('path');
const cors = require('cors');

class SSTableAPIServer {
  constructor() {
    this.app = express();
    this.readers = new Map(); // Cache of SSTable readers
    this.setupMiddleware();
    this.setupRoutes();
    this.setupErrorHandling();
  }

  setupMiddleware() {
    this.app.use(cors());
    this.app.use(express.json());
    this.app.use(express.urlencoded({ extended: true }));
    
    // Request logging
    this.app.use((req, res, next) => {
      console.log(`${new Date().toISOString()} ${req.method} ${req.path}`);
      next();
    });
  }

  setupRoutes() {
    // Health check
    this.app.get('/health', (req, res) => {
      res.json({ 
        status: 'healthy', 
        service: 'CQLite SSTable API',
        version: '1.0.0',
        capabilities: ['direct_sstable_queries', 'streaming', 'aggregations']
      });
    });

    // List available SSTable files
    this.app.get('/api/sstables', this.listSSTableFiles.bind(this));

    // Open SSTable for querying
    this.app.post('/api/sstables/:name/open', this.openSSTable.bind(this));

    // Execute query on SSTable
    this.app.post('/api/sstables/:name/query', this.executeQuery.bind(this));

    // Stream query results
    this.app.get('/api/sstables/:name/stream', this.streamQuery.bind(this));

    // Get SSTable schema
    this.app.get('/api/sstables/:name/schema', this.getSchema.bind(this));

    // Get SSTable statistics
    this.app.get('/api/sstables/:name/stats', this.getStats.bind(this));

    // Close SSTable reader
    this.app.delete('/api/sstables/:name', this.closeSSTable.bind(this));

    // Batch query multiple SSTable files
    this.app.post('/api/batch-query', this.batchQuery.bind(this));

    // Query builder endpoint
    this.app.post('/api/query-builder', this.buildQuery.bind(this));

    // API documentation
    this.app.get('/api/docs', this.apiDocs.bind(this));
  }

  async listSSTableFiles(req, res) {
    try {
      const fs = require('fs').promises;
      const dataDir = path.join(__dirname, '..', 'test-data');
      
      try {
        const files = await fs.readdir(dataDir);
        const sstableFiles = files
          .filter(file => file.endsWith('-Data.db') || file.endsWith('.db'))
          .map(file => ({
            name: file.replace(/(-Data)?\.db$/, ''),
            filename: file,
            path: path.join(dataDir, file)
          }));

        res.json({
          sstables: sstableFiles,
          count: sstableFiles.length
        });
      } catch (error) {
        res.json({ sstables: [], count: 0, note: 'No test data directory found' });
      }
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async openSSTable(req, res) {
    try {
      const { name } = req.params;
      const { schema, compression = 'lz4', cacheSize = 64 } = req.body;

      if (this.readers.has(name)) {
        return res.json({ message: 'SSTable already open', name });
      }

      const sstablePath = this.getSSTablePath(name);
      const schemaPath = schema || this.getSchemaPath(name);

      const reader = new SSTableReader(sstablePath, {
        schema: schemaPath,
        compression,
        cacheSize,
        enableBloomFilter: true
      });

      this.readers.set(name, reader);

      const stats = await reader.getStats();
      const schemaInfo = await reader.getSchema();

      res.json({
        message: 'SSTable opened successfully',
        name,
        stats,
        schema: schemaInfo
      });

    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  }

  async executeQuery(req, res) {
    try {
      const { name } = req.params;
      const { sql, limit, timeout = 30000, format = 'json' } = req.body;

      const reader = this.getReader(name);
      
      const startTime = Date.now();
      const result = await reader.query(sql, { limit, timeout });
      const totalTime = Date.now() - startTime;

      // Different response formats
      let response;
      switch (format) {
        case 'csv':
          response = this.formatAsCSV(result);
          res.setHeader('Content-Type', 'text/csv');
          res.setHeader('Content-Disposition', `attachment; filename="${name}-query-results.csv"`);
          return res.send(response);
        
        case 'xml':
          response = this.formatAsXML(result);
          res.setHeader('Content-Type', 'application/xml');
          return res.send(response);
        
        default:
          response = {
            query: sql,
            results: result.rows,
            metadata: {
              rowCount: result.rowCount,
              executionTime: result.executionTime,
              totalTime,
              sstable: name
            }
          };
      }

      res.json(response);

    } catch (error) {
      res.status(400).json({ error: error.message, code: error.code });
    }
  }

  async streamQuery(req, res) {
    try {
      const { name } = req.params;
      const { sql, format = 'json' } = req.query;

      if (!sql) {
        return res.status(400).json({ error: 'SQL query parameter required' });
      }

      const reader = this.getReader(name);
      
      // Set appropriate headers for streaming
      res.setHeader('Content-Type', format === 'json' ? 'application/x-ndjson' : 'text/plain');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');

      const stream = reader.queryStream(sql);
      let rowCount = 0;

      stream.on('data', (row) => {
        rowCount++;
        
        if (format === 'json') {
          res.write(JSON.stringify(row) + '\n');
        } else {
          // CSV format
          if (rowCount === 1) {
            res.write(Object.keys(row).join(',') + '\n');
          }
          res.write(Object.values(row).map(v => `"${v}"`).join(',') + '\n');
        }
      });

      stream.on('end', () => {
        res.write(`\n# Query completed: ${rowCount} rows streamed\n`);
        res.end();
      });

      stream.on('error', (error) => {
        res.write(`\n# Error: ${error.message}\n`);
        res.status(500).end();
      });

    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  }

  async getSchema(req, res) {
    try {
      const { name } = req.params;
      const reader = this.getReader(name);
      
      const schema = await reader.getSchema();
      res.json(schema);

    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  }

  async getStats(req, res) {
    try {
      const { name } = req.params;
      const reader = this.getReader(name);
      
      const stats = await reader.getStats();
      res.json(stats);

    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  }

  async closeSSTable(req, res) {
    try {
      const { name } = req.params;
      
      if (!this.readers.has(name)) {
        return res.status(404).json({ error: 'SSTable not open' });
      }

      const reader = this.readers.get(name);
      await reader.close();
      this.readers.delete(name);

      res.json({ message: 'SSTable closed successfully', name });

    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  }

  async batchQuery(req, res) {
    try {
      const { queries, sstables, schema } = req.body;

      if (!Array.isArray(queries) || !Array.isArray(sstables)) {
        return res.status(400).json({ error: 'queries and sstables must be arrays' });
      }

      const results = [];

      for (const sstableName of sstables) {
        const sstablePath = this.getSSTablePath(sstableName);
        const schemaPath = schema || this.getSchemaPath(sstableName);

        const reader = new SSTableReader(sstablePath, {
          schema: schemaPath,
          compression: 'lz4',
          cacheSize: 32
        });

        try {
          for (const sql of queries) {
            const startTime = Date.now();
            const result = await reader.query(sql);
            const totalTime = Date.now() - startTime;

            results.push({
              sstable: sstableName,
              query: sql,
              results: result.rows,
              metadata: {
                rowCount: result.rowCount,
                executionTime: result.executionTime,
                totalTime
              }
            });
          }
        } finally {
          await reader.close();
        }
      }

      res.json({
        batchResults: results,
        summary: {
          totalQueries: queries.length * sstables.length,
          totalResults: results.reduce((sum, r) => sum + r.metadata.rowCount, 0),
          totalTime: results.reduce((sum, r) => sum + r.metadata.totalTime, 0)
        }
      });

    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  }

  async buildQuery(req, res) {
    try {
      const { table, select, where, orderBy, limit, groupBy } = req.body;

      let sql = `SELECT ${select ? select.join(', ') : '*'} FROM ${table}`;
      
      if (where && where.length > 0) {
        sql += ` WHERE ${where.join(' AND ')}`;
      }
      
      if (groupBy && groupBy.length > 0) {
        sql += ` GROUP BY ${groupBy.join(', ')}`;
      }
      
      if (orderBy) {
        sql += ` ORDER BY ${orderBy.column} ${orderBy.direction || 'ASC'}`;
      }
      
      if (limit) {
        sql += ` LIMIT ${limit}`;
      }

      // Validate the generated query
      try {
        Utils.validateQuery(sql);
        res.json({ 
          sql, 
          valid: true,
          note: 'Query built successfully. Use /api/sstables/{name}/query to execute.'
        });
      } catch (error) {
        res.status(400).json({ 
          sql, 
          valid: false, 
          error: error.message 
        });
      }

    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  }

  apiDocs(req, res) {
    const docs = {
      title: 'CQLite SSTable API',
      description: 'Revolutionary REST API for direct SSTable querying',
      version: '1.0.0',
      endpoints: {
        'GET /health': 'Health check',
        'GET /api/sstables': 'List available SSTable files',
        'POST /api/sstables/:name/open': 'Open SSTable for querying',
        'POST /api/sstables/:name/query': 'Execute SQL query on SSTable',
        'GET /api/sstables/:name/stream': 'Stream query results',
        'GET /api/sstables/:name/schema': 'Get SSTable schema',
        'GET /api/sstables/:name/stats': 'Get SSTable statistics',
        'DELETE /api/sstables/:name': 'Close SSTable reader',
        'POST /api/batch-query': 'Execute queries on multiple SSTable files',
        'POST /api/query-builder': 'Build and validate SQL queries'
      },
      examples: {
        openSSTable: {
          method: 'POST',
          url: '/api/sstables/users/open',
          body: {
            schema: 'users_schema.json',
            compression: 'lz4',
            cacheSize: 64
          }
        },
        executeQuery: {
          method: 'POST',
          url: '/api/sstables/users/query',
          body: {
            sql: 'SELECT name, email FROM users WHERE age > 25 LIMIT 10',
            format: 'json'
          }
        },
        streamQuery: {
          method: 'GET',
          url: '/api/sstables/users/stream?sql=SELECT * FROM users WHERE active = true&format=json'
        }
      }
    };

    res.json(docs);
  }

  setupErrorHandling() {
    this.app.use((error, req, res, next) => {
      console.error('API Error:', error);
      
      if (error instanceof CQLiteError) {
        res.status(400).json({
          error: error.message,
          code: error.code,
          details: error.details
        });
      } else {
        res.status(500).json({
          error: 'Internal server error',
          message: error.message
        });
      }
    });

    // 404 handler
    this.app.use((req, res) => {
      res.status(404).json({
        error: 'Endpoint not found',
        availableEndpoints: [
          'GET /health',
          'GET /api/sstables',
          'POST /api/sstables/:name/open',
          'POST /api/sstables/:name/query',
          'GET /api/sstables/:name/stream',
          'GET /api/docs'
        ]
      });
    });
  }

  getReader(name) {
    if (!this.readers.has(name)) {
      throw new CQLiteError(`SSTable '${name}' is not open. Open it first with POST /api/sstables/${name}/open`, 'SSTABLE_NOT_OPEN');
    }
    return this.readers.get(name);
  }

  getSSTablePath(name) {
    const dataDir = path.join(__dirname, '..', 'test-data');
    const possibleFiles = [
      `${name}-Data.db`,
      `${name}.db`,
      name
    ];

    for (const file of possibleFiles) {
      const fullPath = path.join(dataDir, file);
      if (require('fs').existsSync(fullPath)) {
        return fullPath;
      }
    }

    throw new CQLiteError(`SSTable file not found for '${name}'`, 'FILE_NOT_FOUND');
  }

  getSchemaPath(name) {
    const dataDir = path.join(__dirname, '..', 'test-data');
    const possibleFiles = [
      `${name}_schema.json`,
      `${name}.schema.json`,
      'schema.json'
    ];

    for (const file of possibleFiles) {
      const fullPath = path.join(dataDir, file);
      if (require('fs').existsSync(fullPath)) {
        return fullPath;
      }
    }

    throw new CQLiteError(`Schema file not found for '${name}'`, 'SCHEMA_NOT_FOUND');
  }

  formatAsCSV(result) {
    if (result.rows.length === 0) return '';
    
    const headers = Object.keys(result.rows[0]);
    const csvRows = [headers.join(',')];
    
    result.rows.forEach(row => {
      const values = headers.map(header => {
        const value = row[header];
        return `"${value !== null && value !== undefined ? value.toString().replace(/"/g, '""') : ''}"`;
      });
      csvRows.push(values.join(','));
    });
    
    return csvRows.join('\n');
  }

  formatAsXML(result) {
    let xml = '<?xml version="1.0" encoding="UTF-8"?>\n<results>\n';
    
    result.rows.forEach((row, index) => {
      xml += `  <row index="${index}">\n`;
      Object.entries(row).forEach(([key, value]) => {
        xml += `    <${key}>${value !== null && value !== undefined ? value.toString().replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : ''}</${key}>\n`;
      });
      xml += '  </row>\n';
    });
    
    xml += '</results>';
    return xml;
  }

  async start(port = 3000) {
    return new Promise((resolve) => {
      this.server = this.app.listen(port, () => {
        console.log(`🚀 CQLite SSTable API Server running on port ${port}`);
        console.log(`📖 API Documentation: http://localhost:${port}/api/docs`);
        console.log(`❤️  Health Check: http://localhost:${port}/health`);
        resolve();
      });
    });
  }

  async stop() {
    // Close all open readers
    for (const [name, reader] of this.readers) {
      try {
        await reader.close();
        console.log(`Closed SSTable reader: ${name}`);
      } catch (error) {
        console.error(`Error closing reader ${name}:`, error.message);
      }
    }
    this.readers.clear();

    // Close the server
    if (this.server) {
      return new Promise((resolve) => {
        this.server.close(resolve);
      });
    }
  }
}

// CLI usage
async function main() {
  const port = process.env.PORT || 3000;
  const server = new SSTableAPIServer();

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\n🛑 Shutting down gracefully...');
    await server.stop();
    console.log('✅ Server stopped');
    process.exit(0);
  });

  await server.start(port);
}

if (require.main === module) {
  main().catch(error => {
    console.error('Failed to start server:', error.message);
    process.exit(1);
  });
}

module.exports = { SSTableAPIServer };