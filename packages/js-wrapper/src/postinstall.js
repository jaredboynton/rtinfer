'use strict';

// postinstall: install + load the always-on LaunchAgent so rtinferd is
// running immediately after `npm i -g @jaredboynton/rtinfer`. Best-effort:
// a failure here (no binary for this platform, CI sandbox, non-macOS) must
// NOT fail the install. The daemon can always be started manually with
// `rtinferd install` or `rtinferd serve`.

const path = require('path');
const fs = require('fs');
const { spawnRaw } = require('./index');

if (process.env.RTINFER_SKIP_POSTINSTALL === '1') {
  process.exit(0);
}

// Pin the LaunchAgent to the STABLE npm global bin shim (this very wrapper),
// not the versioned native binary, so in-daemon self-update is a no-op for the
// plist: npm rewrites the shim in place and launchd respawns it unchanged.
function npmGlobalShim() {
  // npm exposes the install prefix to lifecycle scripts; the global bin shim
  // lives at <prefix>/bin/rtinferd on POSIX.
  const prefix = process.env.npm_config_prefix || process.env.PREFIX;
  if (prefix) {
    const shim = path.join(prefix, 'bin', 'rtinferd');
    if (fs.existsSync(shim)) return shim;
  }
  return null;
}

try {
  const shim = npmGlobalShim();
  const env = shim ? { ...process.env, RTINFER_LAUNCH_BIN: shim } : process.env;
  const result = spawnRaw(['install'], { env });
  if (result.error || (result.status != null && result.status !== 0)) {
    console.error('[rtinfer] postinstall: daemon not installed automatically; run `rtinferd install` to enable the always-on service.');
  }
} catch (e) {
  console.error(`[rtinfer] postinstall skipped: ${e.message}`);
  console.error('[rtinfer] run `rtinferd install` (macOS) or `rtinferd serve` to start the daemon.');
}
// Always exit 0 so npm install never fails on the daemon side-effect.
process.exit(0);

