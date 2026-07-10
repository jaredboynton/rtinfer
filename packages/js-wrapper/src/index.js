'use strict';

// Resolve the native rtinferd binary from the platform package (or a vendored
// fallback) and exec it. Mirrors the cse-tools js-wrapper pattern.

const path = require('path');
const fs = require('fs');
const child_process = require('child_process');

function platformPackageName() {
  return `@jaredboynton/rtinfer-${process.platform}-${process.arch}`;
}

function nativeBinaryName(platform) {
  return platform === 'win32' ? 'rtinferd.exe' : 'rtinferd';
}

function resolveCoreBinary() {
  const platformKey = `${process.platform}-${process.arch}`;
  const supportedPlatforms = new Set(['darwin-arm64', 'linux-arm64', 'linux-x64']);
  const binaryName = nativeBinaryName(process.platform);

  if (supportedPlatforms.has(platformKey)) {
    try {
      const pkgJson = require.resolve(`${platformPackageName()}/package.json`);
      const candidate = path.join(path.dirname(pkgJson), 'bin', binaryName);
      if (fs.existsSync(candidate)) {
        fs.accessSync(candidate, fs.constants.X_OK);
        return candidate;
      }
    } catch (e) {
      if (e && e.code === 'EACCES') throw new Error('native runtime is not executable');
    }
  }

  const vendorCandidate = path.resolve(__dirname, '..', 'vendor', binaryName);
  if (fs.existsSync(vendorCandidate)) {
    fs.accessSync(vendorCandidate, fs.constants.X_OK);
    return vendorCandidate;
  }

  throw new Error(`native runtime is unavailable for this platform: ${platformKey}`);
}

function spawnRaw(args, options) {
  return child_process.spawnSync(resolveCoreBinary(), args || [], {
    stdio: 'inherit',
    ...(options || {}),
  });
}

module.exports = { resolveCoreBinary, spawnRaw };
