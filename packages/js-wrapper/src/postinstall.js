'use strict';

// postinstall: install + load the always-on LaunchAgent so rtinferd is
// running immediately after `npm i -g @jaredboynton/rtinfer`. Best-effort:
// a failure here (no binary for this platform, CI sandbox, non-macOS) must
// NOT fail the install. The daemon can always be started manually with
// `rtinferd install` or `rtinferd serve`.

const { spawnRaw } = require('./index');

if (process.env.RTINFER_SKIP_POSTINSTALL === '1') {
  process.exit(0);
}

try {
  const result = spawnRaw(['install']);
  if (result.error || (result.status != null && result.status !== 0)) {
    console.error('[rtinfer] postinstall: daemon not installed automatically; run `rtinferd install` to enable the always-on service.');
  }
} catch (e) {
  console.error(`[rtinfer] postinstall skipped: ${e.message}`);
  console.error('[rtinfer] run `rtinferd install` (macOS) or `rtinferd serve` to start the daemon.');
}
// Always exit 0 so npm install never fails on the daemon side-effect.
process.exit(0);
