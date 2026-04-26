/* Platform-aware native module loader for @sqd-pipes/settle-stream */
const { existsSync } = require('node:fs')
const { join } = require('node:path')

const { platform, arch } = process

let nativeBinding = null
let loadError = null

function getPlatformFile() {
  const suffixes = {
    'darwin-x64': 'darwin-x64',
    'darwin-arm64': 'darwin-arm64',
    'linux-x64': 'linux-x64-gnu',
    'linux-arm64': 'linux-arm64-gnu',
  }

  const key = `${platform}-${arch}`
  // @ts-ignore
  return suffixes[key] ? `settle-stream.${suffixes[key]}.node` : null
}

// Try platform-specific file (e.g. settle-stream.linux-x64-gnu.node)
const platformFile = getPlatformFile()
if (platformFile) {
  const platformPath = join(__dirname, platformFile)
  if (existsSync(platformPath)) {
    try {
      nativeBinding = require(platformPath)
    } catch (e) {
      loadError = e
    }
  }
}

// Fallback: try unqualified .node file (local dev build)
if (!nativeBinding) {
  const localFile = join(__dirname, 'settle-stream.node')
  if (existsSync(localFile)) {
    try {
      nativeBinding = require(localFile)
    } catch (e) {
      loadError = e
    }
  }
}

if (!nativeBinding) {
  const help = [
    `Failed to load native binding for ${platform}-${arch}.`,
    platformFile ? `Looked for: ${platformFile}` : `Unsupported platform: ${platform}-${arch}`,
    '',
    // @ts-ignore
    loadError ? `Error: ${loadError.message}` : '',
    '',
    'Build from source: npm run build',
  ].join('\n')
  throw new Error(help)
}

module.exports = nativeBinding
