/* Platform-aware native module loader for @settle/stream */
/* Pattern: local .node file (dev) → platform npm package (production) */

const {existsSync} = require('node:fs')
const {join} = require('node:path')

const suffixes = {
    'darwin-arm64': 'darwin-arm64',
    'darwin-x64': 'darwin-x64',
    'linux-x64': 'linux-x64-gnu',
    'linux-arm64': 'linux-arm64-gnu',
}

const key = `${process.platform}-${process.arch}`
const suffix = suffixes[key]

if (!suffix) {
    throw new Error(
        `Unsupported platform: ${key}. ` +
        `Supported: ${Object.keys(suffixes).join(', ')}`
    )
}

const nodeFile = `settle.${suffix}.node`
let nativeBinding

// 1. Local .node file (dev build / bundled package)
const localPath = join(__dirname, nodeFile)
if (existsSync(localPath)) {
    nativeBinding = require(localPath)
}

// 2. Platform npm package (production install)
if (!nativeBinding) {
    try {
        nativeBinding = require(`@settle/stream-${suffix}`)
    } catch {
    }
}

if (!nativeBinding) {
    throw new Error(
        `Failed to load native binding for ${key}.\n` +
        `Tried:\n` +
        `  - ${localPath}\n` +
        `  - @settle/stream-${suffix}\n\n` +
        `Run: npm install @settle/stream`
    )
}

module.exports = nativeBinding
