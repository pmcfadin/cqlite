/**
 * Error wrapper module for CQLite Node.js bindings.
 *
 * This module provides utilities for parsing error metadata from
 * native error messages and attaching them as properties.
 *
 * Issue #297: Error Mapping Implementation
 */

/**
 * Parse error metadata from a message string.
 *
 * The Rust layer encodes metadata in the message using null-byte separators:
 * "Human-readable message\0code=IO\0category=System\0isRecoverable=true"
 *
 * @param {string} message - The error message from native code
 * @returns {Object} Parsed metadata with code, category, isRecoverable, and message
 */
function parseErrorMetadata(message) {
  if (!message || typeof message !== 'string') {
    return {
      code: 'INTERNAL',
      category: 'Internal',
      isRecoverable: false,
      message: String(message || 'Unknown error'),
    };
  }

  // Split by null bytes
  const parts = message.split('\0');
  const humanMessage = parts[0];

  // Default values
  let code = 'INTERNAL';
  let category = 'Internal';
  let isRecoverable = false;

  // Parse metadata from remaining parts
  for (let i = 1; i < parts.length; i++) {
    const part = parts[i];
    if (part.startsWith('code=')) {
      code = part.slice(5);
    } else if (part.startsWith('category=')) {
      category = part.slice(9);
    } else if (part.startsWith('isRecoverable=')) {
      isRecoverable = part.slice(14) === 'true';
    }
  }

  return {
    code,
    category,
    isRecoverable,
    message: humanMessage,
  };
}

/**
 * Enhance an Error object with CQLite metadata properties.
 *
 * @param {Error} error - The error to enhance
 * @returns {Error} The enhanced error with code, category, and isRecoverable properties
 */
function enhanceError(error) {
  if (!error || typeof error.message !== 'string') {
    return error;
  }

  const metadata = parseErrorMetadata(error.message);

  // Update the message to the human-readable part only
  error.message = metadata.message;

  // Add properties
  error.code = metadata.code;
  error.category = metadata.category;
  error.isRecoverable = metadata.isRecoverable;

  return error;
}

/**
 * Wrap an async function to enhance any thrown errors.
 *
 * @param {Function} fn - The async function to wrap
 * @returns {Function} A wrapped function that enhances errors
 */
function wrapAsync(fn) {
  return async function (...args) {
    try {
      return await fn.apply(this, args);
    } catch (error) {
      throw enhanceError(error);
    }
  };
}

/**
 * Create a wrapped Database class with enhanced error handling.
 *
 * @param {Function} NativeDatabase - The native Database class
 * @returns {Function} A wrapped Database class
 */
function createWrappedDatabase(NativeDatabase) {
  class Database {
    constructor(native) {
      this._native = native;
    }

    static async open(dataDir, options) {
      try {
        const native = await NativeDatabase.open(dataDir, options);
        return new Database(native);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async execute(query) {
      try {
        return await this._native.execute(query);
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async getStats() {
      try {
        return await this._native.getStats();
      } catch (error) {
        throw enhanceError(error);
      }
    }

    async close() {
      try {
        return await this._native.close();
      } catch (error) {
        throw enhanceError(error);
      }
    }

    get isClosed() {
      return this._native.isClosed;
    }
  }

  return Database;
}

module.exports = {
  parseErrorMetadata,
  enhanceError,
  wrapAsync,
  createWrappedDatabase,
};
