#!/usr/bin/env node
// Print a digest of a Chronicle store as its own read gives it, on every branch, so a store can be compared with
// itself before and after compact-set-states.py by `diff`. For each branch: every record that is not a state update,
// the raw bytes of each registered state's value (a tree state by its entries), and eight reads of each state that is
// no tree, at sequences spread over the branch. Give it the --state patterns that compact-set-states.py gets, and it
// leaves out the reads at those sequences for the states they name, whose old values the tool frees. So the two
// outputs of a run that went right are equal. It also prints the store's current branch, and reads each branch
// through a view where the binding has one, so it switches none. It prints names, counts and digests, and no
// content. It exits 1 when a read fails.
//
// Two limits. chronicle's API lists only registered states, so a state with no registration isn't read. And the check
// opens the store as its host does: that open saves state.bin and branches.bin, and it cuts off a torn tail of the log.
// What the open reports goes to stderr, outside the digest. A torn end of the log shows at the first open after an
// unclean stop alone, so a check after a run would differ from the check before it for that reason alone.
//
// Usage: CHRONICLE_MODULE=<path to @animalabs/chronicle> node state-digests.mjs <store-dir> [--state <pattern> ...]
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';

const require = createRequire(import.meta.url);
const mod = process.env.CHRONICLE_MODULE;
if (!mod) { console.error('set CHRONICLE_MODULE to the @animalabs/chronicle package directory'); process.exit(2); }
const [path, ...rest] = process.argv.slice(2);
const patterns = [];
for (let i = 0; i < rest.length; i += 2) {
  if (rest[i] !== '--state' || rest[i + 1] === undefined) { console.error(`unknown argument ${rest[i]}`); process.exit(2); }
  patterns.push(patternToRegExp(rest[i + 1]));
}
if (!path) { console.error('usage: node state-digests.mjs <store-dir> [--state <pattern> ...]'); process.exit(2); }
const named = (id) => patterns.some((pattern) => pattern.test(id));

// A pattern is literal text in which each * stands for any text, as in compact-set-states.py.
function patternToRegExp(pattern) {
  return new RegExp(`^${pattern.split('*').map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('.*')}$`, 's');
}

const { JsStore } = require(resolve(mod));
const sha = (value) => createHash('sha256').update(value).digest('hex').slice(0, 16);
// The binding frees a Buffer that it returned only when the event loop turns, so the check turns it after each read
// that returns bytes, and holds one record at a time.
const turn = () => new Promise((done) => setImmediate(done));
const bytes = (raw) => (raw == null ? 'absent' : `${sha(raw)} bytes=${raw.length}`);

const store = JsStore.open({ path });
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
// A binding before 0.5.0 reports truncatedAt, droppedBytes and validRecords alone, so a field it lacks prints as -.
const recovery = store.recovery();
const field = (value) => value ?? '-';
if (recovery) {
  console.error('This open reported: '
    + `droppedBytes=${field(recovery.droppedBytes)} undecodable=${field(recovery.undecodableStateUpdates)} `
    + `onMissingBranches=${field(recovery.updatesOnMissingBranches)} unregistered=`
    + `${recovery.unregisteredStates?.map(({ id, updates }) => `${id}:${updates}`).sort().join(',') || '-'}. `
    + 'Dropped bytes are a torn end of the log, which this open cut off. The other fields count records typed '
    + 'state_update after the newest head in state.bin: ones that do not decode and ones on missing branches are '
    + 'skipped at every open, and ones of unregistered states were applied by this open.');
}
const stats = store.stats();
out.push(`blobs=${stats.blobCount} branches=${stats.branchCount} stateSlots=${stats.stateSlotCount}`);
const found = store.currentBranch().name;
out.push(`current\t${found}`);
const byId = (a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
const branches = store.listBranches().map((branch) => branch.name).sort();
// A view reads a branch without switching the store's current branch, which a save would keep. A binding before
// 0.5.0 has no view, so the check switches there, and switches back at the end.
const pinned = typeof store.view === 'function';
try {
  for (const branch of branches) {
    const on = pinned ? store.view(branch) : (store.switchBranch(branch), store);
    const records = createHash('sha256');
    const types = new Set();
    let count = 0;
    let from = 0;
    for (;;) {
      const page = on.query({ fromSequence: from, limit: 1 });
      await turn();
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
    const top = on.currentSequence();
    out.push(`branch ${branch}\tsequence=${top}\trecords=${count}\t${records.digest('hex').slice(0, 16)}\ttypes=${types.size}`);
    for (const { id, strategy } of on.listStates().sort(byId)) {
      const value = read(() => (strategy === 'tree'
        ? `tree ${sha(JSON.stringify(on.treeList(id)))}`
        : `${strategy} ${bytes(on.getState(id))} items=${on.getStateLen(id) ?? '-'}`));
      out.push(`  ${branch}\t${id}\t${value}`);
      await turn();
      if (strategy !== 'tree' && !named(id)) {
        const history = [];
        for (let k = 1; k <= 8; k++) {
          const at = Math.floor((top * k) / 8);
          history.push(read(() => {
            const raw = on.getStateAt(id, at);
            return raw == null ? 'absent' : sha(raw);
          }));
          await turn();
        }
        out.push(`  ${branch}\t${id}\tat eight sequences ${history.join(' ')}`);
      }
    }
  }
} finally {
  if (!pinned) store.switchBranch(found);
  store.close();
}
console.log(out.join('\n'));
if (failed) {
  console.error(`${failed} read(s) failed`);
  process.exit(1);
}
