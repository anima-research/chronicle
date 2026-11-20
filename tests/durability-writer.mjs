import { closeSync, existsSync, fsyncSync, openSync, renameSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { JsStore } from '../index.js';

const root = process.argv[2];
const path = join(root, 'store');
const control = join(root, 'control');
const mark = name => existsSync(`/chronicle-mark/${name}`);
const entry = blobHash => ({ blobHash, size: 4, mode: 0o644 });

let store;
let blobHash;
let mainSequence;

const barriers = [
  ['sync on a new store after one appendJson', () => {
    store = JsStore.create({ path });
    store.appendJson('journal', { n: 0 });
    store.sync();
  }],
  ['sync after registerState', () => {
    for (const [id, strategy] of [['mode', 'snapshot'], ['raw', 'snapshot'], ['messages', 'append_log'],
      ['identified', 'append_log'], ['files', 'tree']]) {
      store.registerState({ id, strategy });
    }
    store.sync();
  }],
  ['sync after appendJson', () => {
    store.appendJson('journal', { n: 1 });
    store.sync();
  }],
  ['sync after setStateJson', () => {
    store.setStateJson('mode', { mode: 'active' });
    store.sync();
  }],
  ['sync after setState', () => {
    store.setState('raw', Buffer.from('raw bytes'));
    store.sync();
  }],
  ['sync after appendToStateJson', () => {
    store.appendToStateJson('messages', { channel: 'general', text: 'first' });
    store.sync();
  }],
  ['sync after appendToStateJsonWithIdentity', () => {
    store.appendToStateJsonWithIdentity('identified', { text: 'one' }, 'id', 'sequence');
    store.sync();
  }],
  ['sync after appendManyToStateJsonWithIdentity', () => {
    store.appendManyToStateJsonWithIdentity('identified', [{ text: 'two' }, { text: 'three' }], 'id', 'sequence');
    store.sync();
  }],
  ['sync after editStateItem', () => {
    store.editStateItem('messages', 0, Buffer.from(JSON.stringify({ channel: 'general', text: 'edited' })));
    store.sync();
  }],
  ['sync after redactStateItems', () => {
    store.redactStateItems('identified', 0, 1);
    store.sync();
  }],
  ['sync after registerStateFieldIndex', () => {
    store.registerStateFieldIndex('messages', '/channel', 'string');
    store.sync();
  }],
  ['sync after storeBlob', () => {
    blobHash = store.storeBlob(Buffer.from('blob'), 'text/plain');
    store.sync();
  }],
  ['sync after treeSet', () => {
    store.treeSet('files', 'a.txt', entry(blobHash));
    store.sync();
  }],
  ['sync after treeRemove', () => {
    store.treeRemove('files', 'a.txt');
    store.sync();
  }],
  ['sync after treeBatch', () => {
    store.treeBatch('files', [
      { op: 'set', path: 'b.txt', entry: entry(blobHash) },
      { op: 'set', path: 'c.txt', entry: entry(blobHash) },
    ]);
    store.sync();
  }],
  ['sync after compactState', () => {
    store.compactState('messages');
    store.sync();
  }],
  ['sync after updateStateStrategy', () => {
    store.updateStateStrategy({ id: 'messages', strategy: 'append_log', deltaSnapshotEvery: 5, fullSnapshotEvery: 2 });
    store.sync();
  }],
  ['sync after setAutoSnapshot', () => {
    store.setAutoSnapshot(false);
    store.sync();
  }],
  ['sync after createBranch', () => {
    mainSequence = store.currentSequence();
    store.createBranch('plain');
    store.sync();
  }],
  ['sync after switchBranch', () => {
    store.switchBranch('plain');
    store.sync();
  }],
  ['sync after createBranchWithStates', () => {
    store.createBranchWithStates('selective', 'main', { stateIds: ['messages'] });
    store.sync();
  }],
  ['sync after createBranchAt', () => {
    store.createBranchAt('past', 'main', mainSequence);
    store.sync();
  }],
  ['sync after createBranchAtWithStates', () => {
    store.createBranchAtWithStates('past-selective', 'main', mainSequence, { stateIds: ['messages'] });
    store.sync();
  }],
  ['sync after deleteBranch', () => {
    store.deleteBranch('past');
    store.sync();
  }],
  ['sync after a write through a view of a branch that is not the current one', () => {
    store.view('selective').appendToStateJson('messages', { channel: 'general', text: 'through a view' });
    store.sync();
  }],
  ['close with writes pending', () => {
    store.appendJson('journal', { n: 2 });
    store.setStateJson('mode', { mode: 'closing' });
    store.appendToStateJson('messages', { channel: 'general', text: 'pending' });
    store.treeSet('files', 'd.txt', entry(blobHash));
    store.storeBlob(Buffer.from('last'), 'text/plain');
  }, () => store.close()],
  ['sync on a reopened store after appendJson', () => {
    store = JsStore.open({ path });
    store.appendJson('journal', { n: 3 });
    store.sync();
  }],
  ['close on a reopened store with a write pending', () => {
    store.appendJson('journal', { n: 4 });
  }, () => store.close()],
];

console.log(JSON.stringify(barriers.map(([label, , barrier]) => ({ label, pending: barrier !== undefined }))));
mark('begin');
writeFileSync(join(control, 'first'), 'unsynced');
writeFileSync(join(control, 'other'), 'unsynced');
mark('control-1');
const file = openSync(join(control, 'first'), 'r+');
fsyncSync(file);
closeSync(file);
mark('control-2');
const directory = openSync(control, 'r');
fsyncSync(directory);
closeSync(directory);
mark('control-3');
renameSync(join(control, 'other'), join(control, 'third'));
mark('control-4');
barriers.forEach(([, write, barrier], index) => {
  write();
  if (barrier) {
    mark(`pending-${index}`);
    barrier();
  }
  mark(`barrier-${index}`);
});
mark('end');
