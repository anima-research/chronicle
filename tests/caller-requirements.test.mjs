import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const { requirements } = JSON.parse(readFileSync(join(root, 'tests', 'caller-requirements.json'), 'utf8'));

function testFiles() {
  const top = readdirSync(root).filter((name) => name.endsWith('.mjs')).map((name) => join(root, name));
  const below = ['tests', 'src'].flatMap((dir) => readdirSync(join(root, dir), { recursive: true })
    .filter((name) => /\.(mjs|rs)$/.test(name))
    .map((name) => join(root, dir, name)));
  return [...top, ...below];
}

// For each label, the files that name it in a `pins LABEL` or a `differs from LABEL` comment.
function namedInTests() {
  const named = new Map();
  for (const file of testFiles()) {
    const text = readFileSync(file, 'utf8');
    for (const [, kind, label] of text.matchAll(/(?:\/\/|\/\*)\s*(pins|differs from) ([A-Za-z][A-Za-z0-9]*\.\d+)/g)) {
      const entry = named.get(label) ?? { pins: new Set(), differs: new Set() };
      entry[kind === 'pins' ? 'pins' : 'differs'].add(relative(root, file).split('\\').join('/'));
      named.set(label, entry);
    }
  }
  return named;
}

test('caller-requirements.json agrees with the pins and differs comments of the tests', () => {
  const named = namedInTests();
  for (const label of named.keys()) assert.ok(requirements[label], `${label} is named in a test but has no entry`);
  for (const [label, { status, tests }] of Object.entries(requirements)) {
    const { pins, differs } = named.get(label) ?? { pins: new Set(), differs: new Set() };
    const expected = status === 'no test' ? [false, false]
      : status === 'differs' ? [pins.size > 0 || differs.size > 0, true]
      : [true, false];
    assert.deepEqual([pins.size > 0 || differs.size > 0, differs.size > 0], expected, `${label} is "${status}"`);
    assert.deepEqual([...new Set([...pins, ...differs])].sort(), [...(tests ?? [])].sort(), `the files that name ${label}`);
  }
});
