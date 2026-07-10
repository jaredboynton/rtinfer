#!/usr/bin/env node
'use strict';

const { spawnRaw } = require('../src/index');

const result = spawnRaw(process.argv.slice(2));
if (result.error) {
  console.error('error: rtinferd native runtime failed to start');
  process.exit(1);
}
process.exit(result.status == null ? 1 : result.status);
