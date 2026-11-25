import { test } from 'node:test';
import assert from 'node:assert/strict';
import { lstatSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

test('Opening a used store whose records.log is missing throws Corruption and creates no log', (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-missing-log-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, 'store');
  const store = JsStore.create({ path });
  store.registerState({ id: 'messages', strategy: 'snapshot' });
  store.setStateJson('messages', { n: 1 });
  store.close();
  rmSync(join(path, 'records.log'));

  for (const open of [(config) => JsStore.open(config), (config) => JsStore.openOrCreate(config)]) {
    assert.throws(() => open({ path }), (error) => {
      assert.equal(error.code, 'Corruption');
      assert.match(error.message, /records\.log is missing/);
      assert.ok(error.message.includes(path), error.message);
      return true;
    });
    assert.throws(() => lstatSync(join(path, 'records.log')), { code: 'ENOENT' });
  }
});
