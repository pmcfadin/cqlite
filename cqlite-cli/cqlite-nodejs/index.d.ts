/**
 * CQLite - Direct SSTable querying for Cassandra
 * The first-ever NodeJS package for direct SSTable SELECT queries!
 */

export interface SSTableOptions {
  /** Path to schema definition file */
  schema: string;
  /** Compression algorithm used in SSTable */
  compression?: 'lz4' | 'snappy' | 'none';
  /** Cache size in MB for better performance */
  cacheSize?: number;
  /** Enable bloom filter optimizations */
  enableBloomFilter?: boolean;
}

export interface QueryOptions {
  /** Maximum number of rows to return */
  limit?: number;
  /** Query timeout in milliseconds */
  timeout?: number;
  /** Enable streaming mode for large results */
  streaming?: boolean;
  /** Skip bloom filter checks (for debugging) */
  skipBloomFilter?: boolean;
}

export interface QueryResult {
  /** Array of result rows */
  rows: Record<string, any>[];
  /** Total number of rows returned */
  rowCount: number;
  /** Query execution time in milliseconds */
  executionTime: number;
  /** Statistics about the query execution */
  stats?: {
    blocksRead: number;
    cacheHits: number;
    cacheMisses: number;
  };
}

export interface SchemaDefinition {
  /** Table name */
  table: string;
  /** Column definitions */
  columns: {
    name: string;
    type: CQLType;
    primaryKey?: boolean;
    clusteringKey?: boolean;
  }[];
}

// CQL Type mappings for TypeScript
export type CQLText = string;
export type CQLInt = number;
export type CQLBigInt = bigint;
export type CQLBoolean = boolean;
export type CQLFloat = number;
export type CQLDouble = number;
export type CQLDecimal = string;
export type CQLTimestamp = Date;
export type CQLUuid = string;
export type CQLList<T> = T[];
export type CQLSet<T> = Set<T>;
export type CQLMap<K, V> = Map<K, V>;
export type CQLType = 'text' | 'int' | 'bigint' | 'boolean' | 'float' | 'double' | 
                     'decimal' | 'timestamp' | 'uuid' | 'list' | 'set' | 'map';

/**
 * Main SSTable reader class for executing SELECT queries
 */
export class SSTableReader {
  /**
   * Create a new SSTable reader
   * @param path Path to the SSTable file
   * @param options Configuration options
   */
  constructor(path: string, options: SSTableOptions);
  
  /**
   * Execute a SELECT query on the SSTable
   * @param sql SQL SELECT statement
   * @param options Query execution options
   * @returns Promise resolving to query results
   */
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
  
  /**
   * Execute a streaming SELECT query for large result sets
   * @param sql SQL SELECT statement
   * @returns Readable stream of result rows
   */
  queryStream(sql: string): NodeJS.ReadableStream;
  
  /**
   * Get schema information for the SSTable
   * @returns Schema definition
   */
  getSchema(): Promise<SchemaDefinition>;
  
  /**
   * Get statistics about the SSTable
   * @returns Statistics object
   */
  getStats(): Promise<{
    fileSize: number;
    estimatedRows: number;
    compression: string;
    bloomFilterPresent: boolean;
  }>;
  
  /**
   * Close the SSTable reader and free resources
   */
  close(): Promise<void>;
}

/**
 * Error classes for better error handling
 */
export class CQLiteError extends Error {
  code: string;
  details?: any;
  constructor(message: string, code: string, details?: any);
}

export class SchemaError extends CQLiteError {
  constructor(message: string, details?: any);
}

export class QueryError extends CQLiteError {
  constructor(message: string, details?: any);
}

export class FileError extends CQLiteError {
  constructor(message: string, details?: any);
}

/**
 * Utility functions
 */
export namespace Utils {
  /**
   * Validate a CQL SELECT statement
   * @param sql SQL statement to validate
   * @returns true if valid, throws error if invalid
   */
  export function validateQuery(sql: string): boolean;
  
  /**
   * Parse schema from Cassandra schema file
   * @param schemaPath Path to schema file
   * @returns Parsed schema definition
   */
  export function parseSchema(schemaPath: string): Promise<SchemaDefinition>;
  
  /**
   * Convert CQL types to JavaScript types
   * @param value Raw value from SSTable
   * @param cqlType CQL type name
   * @returns Converted JavaScript value
   */
  export function convertCQLValue(value: any, cqlType: CQLType): any;
}

// Type-safe SSTable reader for TypeScript users
export interface TypedSSTableReader<T extends Record<string, any>> {
  query(sql: string, options?: QueryOptions): Promise<{
    rows: T[];
    rowCount: number;
    executionTime: number;
  }>;
  
  queryStream(sql: string): NodeJS.ReadableStream & {
    [Symbol.asyncIterator](): AsyncIterableIterator<T>;
  };
}

/**
 * Create a type-safe SSTable reader
 * @param path Path to SSTable file
 * @param options Configuration options
 * @returns Type-safe reader instance
 */
export function createTypedReader<T extends Record<string, any>>(
  path: string, 
  options: SSTableOptions
): TypedSSTableReader<T>;

// Default export
export default SSTableReader;