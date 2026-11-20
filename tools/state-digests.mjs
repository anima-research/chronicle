#!/usr/bin/env node
// Print a digest of a Chronicle store as its own read gives it, on every branch, so a store can be compared with
// itself before and after compact-set-states.py, or with a copy, by `diff`. For each branch: every record that is not
// a state update, each state's value (a tree state by its entries, a value that is not JSON by its bytes), and eight
// reads of each list state at sequences spread over the branch. It prints names, counts and digests, and no content.
// It leaves the store on the branch where it found it, and exits 1 when a read fails.
//
// Usage: CHRONICLE_MODULE=<path to @animalabs/chronicle> node state-digests.mjs <store-dir>
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';

const require = createRequire(import.meta.url);
const mod = process.env.CHRONICLE_MODULE;
if (!mod) { console.error('set CHRONICLE_MODULE to the @animalabs/chronicle package directory'); process.exit(2); }
const { JsStore } = require(resolve(mod));
const sha = (value) => createHash('sha256').update(value).digest('hex').slice(0, 16);

const store = JsStore.open({ path: process.argv[2] });
const out = [];
let failed = 0;
const read = (what) => {
  try {
    return what();
  } catch (error) {
    failed++;
    return `ERROR ${error.code ?? 'read failed'}`;
  }
};
const recovery = store.recovery();
out.push(`recovery\t${recovery ? `report of ${JSON.stringify(recovery).length} characters` : 'none'}`);
const stats = store.stats();
out.push(`blobs=${stats.blobCount} branches=${stats.branchCount} stateSlots=${stats.stateSlotCount}`);
const found = store.currentBranch().name;
const branches = store.listBranches().map((branch) => branch.name).sort();
try {
  for (const branch of branches) {
    store.switchBranch(branch);
    const records = createHash('sha256');
    const types = new Set();
    let count = 0;
    let from = 0;
    for (;;) {
      const page = store.query({ fromSequence: from, limit: 2000 });
      if (page.length === 0) break;
      for (const record of page) {
        if (record.recordType === 'state_update') continue;
        count++;
        types.add(record.recordType);
        records.update(`${record.id}|${record.sequence}|${record.recordType}|${record.timestamp}|`
          + `${JSON.stringify(record.causedBy ?? null)}|${JSON.stringify(record.linkedTo ?? null)}|`);
        records.update(record.payload ?? '');
      }
      const last = page[page.length - 1].sequence;
      if (last < from) throw new Error('a query by sequence did not advance');
      from = last + 1;
    }
    const top = store.currentSequence();
    out.push(`branch ${branch}\tsequence=${top}\trecords=${count}\t${records.digest('hex').slice(0, 16)}\ttypes=${types.size}`);
    for (const { id, strategy } of store.listStates().sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
      const value = read(() => {
        if (strategy === 'tree') return `tree ${sha(JSON.stringify(store.treeList(id)))}`;
        let text;
        try {
          text = JSON.stringify(store.getStateJson(id));
        } catch {
          const raw = store.getState(id);
          text = raw ? raw.toString('base64') : 'absent';
        }
        return `${strategy} ${sha(text ?? 'undefined')} length=${(text ?? '').length} items=${store.getStateLen(id) ?? '-'}`;
      });
      out.push(`  ${branch}\t${id}\t${value}`);
      if (strategy === 'append_log') {
        const history = [];
        for (let k = 1; k <= 8; k++) {
          const at = Math.floor((top * k) / 8);
          history.push(read(() => sha(JSON.stringify(store.getStateJsonAt(id, at)) ?? 'undefined')));
        }
        out.push(`  ${branch}\t${id}\tat eight sequences ${history.join(' ')}`);
      }
    }
  }
} finally {
  store.switchBranch(found);
  store.close();
}
console.log(out.join('\n'));
if (failed) {
  console.error(`${failed} read(s) failed`);
  process.exit(1);
}
