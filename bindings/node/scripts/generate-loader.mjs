/**
 * Generates the root index.js platform loader for @cqlite/node.
 *
 * napi-rs v3 CLI requires the `type-def` proc-macro feature in napi-derive
 * (via NAPI_TYPE_DEF_TMP_FOLDER) to auto-generate this file. This project uses
 * napi-derive 2.16 which predates that mechanism. Until napi-derive is upgraded,
 * this script generates the equivalent CJS platform loader using the same
 * template that `napi build --platform --js index.js` would produce.
 *
 * Called automatically by the `postbuild` npm script.
 */

import { writeFileSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))

const BINARY_NAME = 'cqlite-node'
const PACKAGE_NAME = '@cqlite/node'

// All identifiers exported from src/lib.rs (pub use declarations).
// Keep in sync with bindings/node/src/lib.rs.
const IDENTS = [
  'ColumnInfo',
  'Database',
  'DatabaseOptions',
  'DatabaseStats',
  'MaintenanceOptions',
  'MaintenanceReport',
  'PreparedStatement',
  'PreparedStatementStats',
  'QueryResult',
  'StreamingConfig',
  'StreamingResult',
  'WriteStats',
  'version',
]

// Platform/arch → npm package suffix mapping (mirrors napi-rs defaults).
// This must stay in sync with the `targets` array in package.json.
const PLATFORM_BINDINGS = `
const { existsSync } = require('fs')
const { join } = require('path')

const { platform, arch } = process

let nativeBinding = null
let localFileExisted = false
let loadError = null

function isMusl() {
  if (!process.report || typeof process.report.getReport !== 'function') {
    try {
      const lddPath = require('child_process').execSync('which ldd').toString().trim()
      return require('fs').readFileSync(lddPath, 'utf8').includes('musl')
    } catch {
      return true
    }
  } else {
    const { glibcVersionRuntime } = process.report.getReport().header
    return !glibcVersionRuntime
  }
}

switch (platform) {
  case 'darwin':
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(join(__dirname, '${BINARY_NAME}.darwin-x64.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./${BINARY_NAME}.darwin-x64.node')
          } else {
            nativeBinding = require('@cqlite/node-darwin-x64')
          }
        } catch (e) {
          loadError = e
        }
        break
      case 'arm64':
        localFileExisted = existsSync(join(__dirname, '${BINARY_NAME}.darwin-arm64.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./${BINARY_NAME}.darwin-arm64.node')
          } else {
            nativeBinding = require('@cqlite/node-darwin-arm64')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(\`Unsupported architecture on macOS: \${arch}\`)
    }
    break
  case 'linux':
    switch (arch) {
      case 'x64':
        if (isMusl()) {
          localFileExisted = existsSync(join(__dirname, '${BINARY_NAME}.linux-x64-musl.node'))
          try {
            if (localFileExisted) {
              nativeBinding = require('./${BINARY_NAME}.linux-x64-musl.node')
            } else {
              nativeBinding = require('@cqlite/node-linux-x64-musl')
            }
          } catch (e) {
            loadError = e
          }
        } else {
          localFileExisted = existsSync(join(__dirname, '${BINARY_NAME}.linux-x64-gnu.node'))
          try {
            if (localFileExisted) {
              nativeBinding = require('./${BINARY_NAME}.linux-x64-gnu.node')
            } else {
              nativeBinding = require('@cqlite/node-linux-x64-gnu')
            }
          } catch (e) {
            loadError = e
          }
        }
        break
      case 'arm64':
        if (isMusl()) {
          localFileExisted = existsSync(join(__dirname, '${BINARY_NAME}.linux-arm64-musl.node'))
          try {
            if (localFileExisted) {
              nativeBinding = require('./${BINARY_NAME}.linux-arm64-musl.node')
            } else {
              nativeBinding = require('@cqlite/node-linux-arm64-musl')
            }
          } catch (e) {
            loadError = e
          }
        } else {
          localFileExisted = existsSync(join(__dirname, '${BINARY_NAME}.linux-arm64-gnu.node'))
          try {
            if (localFileExisted) {
              nativeBinding = require('./${BINARY_NAME}.linux-arm64-gnu.node')
            } else {
              nativeBinding = require('@cqlite/node-linux-arm64-gnu')
            }
          } catch (e) {
            loadError = e
          }
        }
        break
      default:
        throw new Error(\`Unsupported architecture on Linux: \${arch}\`)
    }
    break
  case 'win32':
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(join(__dirname, '${BINARY_NAME}.win32-x64-msvc.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./${BINARY_NAME}.win32-x64-msvc.node')
          } else {
            nativeBinding = require('@cqlite/node-win32-x64-msvc')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(\`Unsupported architecture on Windows: \${arch}\`)
    }
    break
  default:
    throw new Error(\`Unsupported OS: \${platform}, architecture: \${arch}\`)
}

if (!nativeBinding) {
  if (loadError) {
    throw loadError
  }
  throw new Error('Failed to load native binding')
}
`

const exportsLines = IDENTS.map(id => `module.exports.${id} = nativeBinding.${id}`).join('\n')

const content = `/* tslint:disable */
/* eslint-disable */
/* prettier-ignore */

/* auto-generated by scripts/generate-loader.mjs */

${PLATFORM_BINDINGS}
${exportsLines}
`

const outPath = join(__dirname, '..', 'index.js')
writeFileSync(outPath, content, 'utf-8')
console.log('Generated index.js platform loader.')
