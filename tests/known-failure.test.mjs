import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const helper = pathToFileURL(join(import.meta.dirname, 'known-failure.mjs')).href;

function runWith(t, body) {
  const dir = mkdtempSync(join(tmpdir(), 'known-failure-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const file = join(dir, 'case.test.mjs');
  writeFileSync(file, [
    "import { test } from 'node:test';",
    "import assert from 'node:assert/strict';",
    `import { knownFailure } from ${JSON.stringify(helper)};`,
    `test('a known failure', knownFailure('the store does not do it yet', () => { ${body} }));`,
    '',
  ].join('\n'));
  const env = { ...process.env };
  delete env.NODE_TEST_CONTEXT;
  return spawnSync(process.execPath, ['--test', '--test-reporter=tap', file], { encoding: 'utf8', timeout: 60_000, env });
}

test('A known failure whose body throws reports as a todo, and the run passes', (t) => {
  const run = runWith(t, 'assert.equal(1, 2);');
  assert.equal(run.status, 0, run.stdout + run.stderr);
  assert.match(run.stdout, /^not ok 1 - a known failure # TODO the store does not do it yet$/m);
  assert.match(run.stdout, /^# fail 0$/m);
});

test('A known failure whose body passes fails the run with the reason', (t) => {
  const run = runWith(t, 'assert.equal(1, 1);');
  assert.equal(run.status, 1, run.stdout + run.stderr);
  assert.match(run.stdout, /^not ok 1 - a known failure$/m);
  assert.match(run.stdout, /The todo no longer fails here: the store does not do it yet/);
  assert.match(run.stdout, /^# fail 1$/m);
});
