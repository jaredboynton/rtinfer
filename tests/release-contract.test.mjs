import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const root = new URL("../", import.meta.url);
const read = (path) => readFileSync(new URL(path, root), "utf8");

test("linux-x64 is a first-class npm runtime", () => {
  const platform = JSON.parse(read("packages/linux-x64/package.json"));
  const meta = JSON.parse(read("packages/js-wrapper/package.json"));
  const resolver = read("packages/js-wrapper/src/index.js");
  const versionScript = read("scripts/set-version.sh");

  assert.equal(platform.name, "@jaredboynton/rtinfer-linux-x64");
  assert.deepEqual(platform.os, ["linux"]);
  assert.deepEqual(platform.cpu, ["x64"]);
  assert.equal(meta.optionalDependencies[platform.name], meta.version);
  assert.match(resolver, /['"]linux-x64['"]/);
  assert.match(versionScript, /packages\/linux-x64\/package\.json/);
  assert.match(versionScript, /optionalDependencies\.linux-x64/);
});

test("release builds, verifies, and publishes linux-x64 natively", () => {
  const workflow = read(".github/workflows/release.yml");

  assert.match(workflow, /runner:\s*ubuntu-22\.04[\s\S]*target:\s*x86_64-unknown-linux-gnu[\s\S]*platform:\s*linux-x64/);
  assert.match(workflow, /@jaredboynton\/rtinfer-linux-x64/);
  assert.match(workflow, /for platform in darwin-arm64 linux-arm64 linux-x64/);
  assert.match(workflow, /rtinferd \$VERSION/);
  assert.match(workflow, /x86-64\|x86_64/);
  assert.match(workflow, /GLIBC_2\.35/);
  assert.match(workflow, /Smoke-test Linux x64 install from npm/);
  assert.match(workflow, /for attempt[\s\S]*version_output=.*rtinferd --version[\s\S]*done/);
  assert.match(workflow, /cancel-in-progress:\s*false/);
  assert.match(workflow, /actions\/cache\/restore@v4/);
  assert.match(workflow, /resolve-boringssl-prebuild\.sh/);
  assert.doesNotMatch(workflow, /refusing to republish/);
  assert.match(workflow, /verification-only rerun/);
});

test("linux release cache warmer shares release fingerprints", () => {
  const timing = read(".github/workflows/build-timing.yml");
  const release = read(".github/workflows/release.yml");

  assert.match(timing, /workflow_dispatch:/);
  assert.match(timing, /default:\s*["']90["']/);
  assert.match(timing, /aarch64-unknown-linux-gnu/);
  assert.match(timing, /x86_64-unknown-linux-gnu/);
  assert.match(timing, /cargo-\$\{\{ matrix\.target \}\}-\$\{\{ hashFiles\('Cargo\.lock'\) \}\}/);
  assert.match(release, /cargo-\$\{\{ matrix\.target \}\}-\$\{\{ hashFiles\('Cargo\.lock'\) \}\}/);
  assert.match(timing, /resolve-boringssl-prebuild\.sh/);
  assert.match(timing, /actions\/cache\/save@v4/);
});
