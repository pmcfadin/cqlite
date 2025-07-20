const { Readable } = require('stream');

// Import the native module
const {
  SSTableReader: NativeSSTableReader,
  validateQuery,
  parseSchema,
  createCqliteError
} = require('../cqlite.node');

/**
 * Enhanced SSTable Reader with JavaScript conveniences
 */
class SSTableReader extends NativeSSTableReader {
  constructor(path, options) {
    super(path, options);
    this._closed = false;
  }

  /**
   * Execute a SELECT query with enhanced error handling
   */
  async query(sql, options = {}) {
    if (this._closed) {
      throw new CQLiteError('Reader is closed', 'READER_CLOSED');
    }

    try {
      const result = await super.query(sql, options);
      return result;
    } catch (error) {
      throw new QueryError(error.message, { sql, options });
    }
  }

  /**
   * Create a readable stream for large result sets
   */
  queryStream(sql) {
    if (this._closed) {
      throw new CQLiteError('Reader is closed', 'READER_CLOSED');
    }

    return new SSTableQueryStream(this, sql);
  }

  /**
   * Close the reader and mark as closed
   */
  async close() {
    if (!this._closed) {
      await super.close();
      this._closed = true;
    }
  }

  /**
   * Check if reader is closed
   */
  get isClosed() {
    return this._closed;
  }
}

/**
 * Streaming query implementation
 */
class SSTableQueryStream extends Readable {
  constructor(reader, sql) {
    super({ objectMode: true });
    this.reader = reader;
    this.sql = sql;
    this._started = false;
    this._ended = false;
  }

  _read() {
    if (this._started || this._ended) {
      return;
    }

    this._started = true;

    // Use the native streaming method
    this.reader.queryStreamInternal(this.sql, (row) => {
      if (Object.keys(row).length === 0) {
        // Empty object signals end of stream
        this.push(null);
        this._ended = true;
      } else {
        this.push(row);
      }
    });
  }

  _destroy(error, callback) {
    this._ended = true;
    callback(error);
  }
}

/**
 * Enhanced error classes
 */
class CQLiteError extends Error {
  constructor(message, code, details = null) {
    super(message);
    this.name = 'CQLiteError';
    this.code = code;
    this.details = details;
    Error.captureStackTrace(this, this.constructor);
  }
}

class SchemaError extends CQLiteError {
  constructor(message, details = null) {
    super(message, 'SCHEMA_ERROR', details);
    this.name = 'SchemaError';
  }
}

class QueryError extends CQLiteError {
  constructor(message, details = null) {
    super(message, 'QUERY_ERROR', details);
    this.name = 'QueryError';
  }
}

class FileError extends CQLiteError {
  constructor(message, details = null) {
    super(message, 'FILE_ERROR', details);
    this.name = 'FileError';
  }
}

/**
 * Utility functions
 */
const Utils = {
  validateQuery,
  parseSchema,
  
  /**
   * Convert CQL values to appropriate JavaScript types
   */
  convertCQLValue(value, cqlType) {
    switch (cqlType) {
      case 'bigint':
        return BigInt(value);
      case 'timestamp':
        return new Date(value);
      case 'list':
      case 'set':
        return Array.isArray(value) ? value : [value];
      case 'map':
        return value instanceof Map ? value : new Map(Object.entries(value || {}));
      default:
        return value;
    }
  },

  /**
   * Create a schema from a JSON object
   */
  createSchema(tableName, columns) {
    return {
      table: tableName,
      columns: columns.map(col => ({
        name: col.name,
        type: col.type,
        primaryKey: col.primaryKey || false,
        clusteringKey: col.clusteringKey || false
      }))
    };
  },

  /**
   * Validate SSTable file format
   */
  async validateSSTableFile(path) {
    const fs = require('fs').promises;
    try {
      const stats = await fs.stat(path);
      if (!stats.isFile()) {
        throw new FileError(`Path is not a file: ${path}`);
      }
      
      // Check for SSTable file extensions
      const validExtensions = ['-Data.db', '.db', '.sst'];
      const hasValidExtension = validExtensions.some(ext => path.endsWith(ext));
      
      if (!hasValidExtension) {
        console.warn(`Warning: File ${path} doesn't have a typical SSTable extension`);
      }
      
      return true;
    } catch (error) {
      throw new FileError(`Cannot access SSTable file: ${error.message}`, { path });
    }
  }
};

/**
 * Type-safe reader factory function
 */
function createTypedReader(path, options) {
  const reader = new SSTableReader(path, options);
  
  return {
    async query(sql, queryOptions) {
      const result = await reader.query(sql, queryOptions);
      return {
        rows: result.rows,
        rowCount: result.rowCount,
        executionTime: result.executionTime
      };
    },

    queryStream(sql) {
      const stream = reader.queryStream(sql);
      
      // Add async iterator support
      stream[Symbol.asyncIterator] = async function* () {
        for await (const row of stream) {
          yield row;
        }
      };
      
      return stream;
    },

    async getSchema() {
      return reader.getSchema();
    },

    async getStats() {
      return reader.getStats();
    },

    async close() {
      return reader.close();
    }
  };
}

/**
 * Convenience function to quickly query an SSTable
 */
async function quickQuery(sstablePath, schemaPath, sql, options = {}) {
  const reader = new SSTableReader(sstablePath, { schema: schemaPath });
  try {
    const result = await reader.query(sql, options);
    return result;
  } finally {
    await reader.close();
  }
}

/**
 * Batch query multiple SSTable files
 */
async function batchQuery(sstablePaths, schemaPath, sql, options = {}) {
  const results = [];
  
  for (const path of sstablePaths) {
    const reader = new SSTableReader(path, { schema: schemaPath });
    try {
      const result = await reader.query(sql, options);
      results.push({
        file: path,
        ...result
      });
    } finally {
      await reader.close();
    }
  }
  
  return results;
}

module.exports = {
  SSTableReader,
  CQLiteError,
  SchemaError,
  QueryError,
  FileError,
  Utils,
  createTypedReader,
  quickQuery,
  batchQuery,
  
  // Re-export native functions
  validateQuery,
  parseSchema
};