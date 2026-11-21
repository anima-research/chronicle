const message = (text, n) => ({ channel: 'general', text, n });
const entry = blobHash => ({ blobHash, size: 4, mode: 0o644 });
const utf8 = buffer => (buffer === null || buffer === undefined ? null : buffer.toString('utf8'));
const stamped = (id, sequence) => typeof id === 'string' && id !== '' && Number.isSafeInteger(sequence) && sequence >= 0;
const plain = items => (items === null ? null : items.map(({ id, sequence, ...rest }) => ({ ...rest, stamped: stamped(id, sequence) })));
const payloads = (s, type) => s.getRecordIdsByType(type).map(id => JSON.parse(utf8(s.getRecord(id).payload)));
const branchNames = s => s.listBranches().map(({ name }) => name).sort();
const stateList = s => s.listStates().map(({ id, strategy }) => `${id} ${strategy}`).sort();
const files = s => s.treeList('files').map(({ path, blobHash, size, mode }) => ({ path, blobHash, size, mode }));
const branchStates = name => s => {
  if (!s.listBranches().some(branch => branch.name === name)) return null;
  const view = s.view(name);
  return { messages: view.getStateJson('messages'), mode: view.getStateJson('mode') };
};

const journal = s => payloads(s, 'journal');
const messages = s => s.getStateJson('messages');
const identified = s => plain(s.getStateJson('identified'));
const strategyOf = s => s.listStates().find(({ id }) => id === 'messages')?.strategy ?? null;

function threeWrites(s) {
  s.appendJson('journal', { n: 1 });
  s.setStateJson('mode', { mode: 'new' });
  s.appendToStateJson('messages', message('new', 1));
}
const threeReads = { journal, mode: s => s.getStateJson('mode'), messages };

function coldSetup(s, f) {
  [1, 2, 3].forEach(n => s.appendToStateJson('messages', message('more', n)));
  s.registerStateFieldIndex('messages', '/channel', 'string');
  s.registerStateFieldIndex('messages', '/n', 'number');
  f.at = s.currentSequence();
  s.setStateJson('mode', { mode: 'later' });
  f.record = s.getRecordIdsByType('journal')[0];
  s.createBranch('other');
}
const cold = (name, read) => ({ name, kind: 'cold', setup: coldSetup, read });

export const appendJson = { name: 'appendJson', kind: 'write', call: s => s.appendJson('journal', { n: 1 }), reads: { journal } };
export const appendManyToStateJsonWithIdentity = {
  name: 'appendManyToStateJsonWithIdentity',
  kind: 'write',
  call: s => s.appendManyToStateJsonWithIdentity('identified', [{ text: 'two' }, { text: 'three' }], 'id', 'sequence'),
  reads: { identified },
};
export const appendToStateJson = {
  name: 'appendToStateJson', kind: 'write', call: s => s.appendToStateJson('messages', message('new', 1)), reads: { messages },
};
export const appendToStateJsonWithIdentity = {
  name: 'appendToStateJsonWithIdentity',
  kind: 'write',
  call: s => s.appendToStateJsonWithIdentity('identified', { text: 'new' }, 'id', 'sequence'),
  reads: { identified },
};
export const compactState = {
  name: 'compactState',
  kind: 'write',
  setup: s => [1, 2, 3].forEach(n => s.appendToStateJson('messages', message('more', n))),
  call: s => s.compactState('messages'),
  reads: { messages, length: s => s.getStateLen('messages') },
};
export const createBranch = { name: 'createBranch', kind: 'write', call: s => s.createBranch('made'), reads: { branches: branchNames } };
export const createBranchAt = {
  name: 'createBranchAt',
  kind: 'write',
  setup: (s, f) => { f.at = s.currentSequence(); s.setStateJson('mode', { mode: 'later' }); },
  call: (s, f) => s.createBranchAt('made', 'main', f.at),
  reads: { branches: branchNames, made: branchStates('made') },
};
export const createBranchAtWithStates = {
  name: 'createBranchAtWithStates',
  kind: 'write',
  setup: (s, f) => { f.at = s.currentSequence(); s.setStateJson('mode', { mode: 'later' }); },
  call: (s, f) => s.createBranchAtWithStates('made', 'main', f.at, { stateIds: ['messages'] }),
  reads: { branches: branchNames, made: branchStates('made') },
};
export const createBranchWithStates = {
  name: 'createBranchWithStates',
  kind: 'write',
  call: s => s.createBranchWithStates('made', 'main', { stateIds: ['messages'] }),
  reads: { branches: branchNames, made: branchStates('made') },
};
export const deleteBranch = {
  name: 'deleteBranch',
  kind: 'write',
  setup: s => s.createBranch('gone'),
  call: s => s.deleteBranch('gone'),
  reads: { branches: branchNames },
};
export const editStateItem = {
  name: 'editStateItem',
  kind: 'write',
  call: s => s.editStateItem('messages', 0, Buffer.from(JSON.stringify(message('edited', 0)))),
  reads: { messages },
};
export const redactStateItems = {
  name: 'redactStateItems',
  kind: 'write',
  setup: s => s.appendManyToStateJsonWithIdentity('identified', [{ text: 'two' }, { text: 'three' }], 'id', 'sequence'),
  call: s => s.redactStateItems('identified', 0, 2),
  reads: { identified, length: s => s.getStateLen('identified') },
};
export const registerState = {
  name: 'registerState', kind: 'write', call: s => s.registerState({ id: 'added', strategy: 'snapshot' }), reads: { states: stateList },
};
export const registerStateFieldIndex = {
  name: 'registerStateFieldIndex',
  kind: 'write',
  call: s => s.registerStateFieldIndex('messages', '/channel', 'string'),
  reads: { index: s => s.queryStateIndexEq('messages', '/channel', 'general', {}) },
};
export const setAutoSnapshot = {
  name: 'setAutoSnapshot', kind: 'write', call: s => s.setAutoSnapshot(false), reads: { enabled: s => s.autoSnapshotEnabled() },
};
export const setState = {
  name: 'setState', kind: 'write', call: s => s.setState('raw', Buffer.from('new bytes')), reads: { raw: s => utf8(s.getState('raw')) },
};
export const setStateJson = {
  name: 'setStateJson', kind: 'write', call: s => s.setStateJson('mode', { mode: 'new' }), reads: { mode: s => s.getStateJson('mode') },
};
export const storeBlob = {
  name: 'storeBlob',
  kind: 'write',
  freshBlob: true,
  call: s => s.storeBlob(Buffer.from('fresh blob'), 'text/plain'),
  reads: { blob: (s, f) => utf8(s.getBlob(f.fresh)) },
};
export const switchBranch = {
  name: 'switchBranch',
  kind: 'write',
  setup: s => s.createBranch('other'),
  call: s => s.switchBranch('other'),
  reads: { current: s => s.currentBranch().name },
};
export const treeBatch = {
  name: 'treeBatch',
  kind: 'write',
  call: (s, f) => s.treeBatch('files', [
    { op: 'set', path: 'b.txt', entry: entry(f.blob) },
    { op: 'remove', path: 'a.txt' },
  ]),
  reads: { files },
};
export const treeRemove = { name: 'treeRemove', kind: 'write', call: s => s.treeRemove('files', 'a.txt'), reads: { files } };
export const treeSet = { name: 'treeSet', kind: 'write', call: (s, f) => s.treeSet('files', 'b.txt', entry(f.blob)), reads: { files } };
export const updateStateStrategy = {
  name: 'updateStateStrategy',
  kind: 'write',
  call: s => s.updateStateStrategy({ id: 'messages', strategy: 'append_log', deltaSnapshotEvery: 5, fullSnapshotEvery: 2 }),
  reads: { strategy: strategyOf, messages },
  probe: s => {
    for (let n = 0; n < 5; n++) s.appendToStateJson('messages', message('probe', n));
    return s.listStates().find(({ id }) => id === 'messages').opsSinceSnapshot;
  },
};

export const sync = { name: 'sync', kind: 'sync', unsynced: threeWrites, call: s => s.sync(), reads: threeReads };
export const close = { name: 'close', kind: 'close', unsynced: threeWrites, call: s => s.close(), reads: threeReads };

const longMethod = i => (i % 2 === 0 ? 'appendJson' : 'appendToStateJson');
export const long = {
  name: 'long',
  kind: 'long',
  appends: 400,
  method: longMethod,
  call: (s, f, i) => (longMethod(i) === 'appendJson'
    ? s.appendJson('journal', { n: i })
    : s.appendToStateJson('messages', message('long', i))),
  reads: { journal, messages },
};

export const currentBranch = cold('currentBranch', s => s.currentBranch().name);
export const currentSequence = cold('currentSequence', s => s.currentSequence());
export const getBlob = cold('getBlob', (s, f) => utf8(s.getBlob(f.blob)));
export const getRecord = cold('getRecord', (s, f) => {
  const record = s.getRecord(f.record);
  return record && { ...record, payload: utf8(record.payload) };
});
export const getRecordIdsByType = cold('getRecordIdsByType', s => s.getRecordIdsByType('journal'));
export const getState = cold('getState', s => utf8(s.getState('raw')));
export const getStateIndexValueCounts = cold('getStateIndexValueCounts', s => s.getStateIndexValueCounts('messages', '/channel'));
export const getStateItemJson = cold('getStateItemJson', s => s.getStateItemJson('messages', 1));
export const getStateJson = cold('getStateJson', s => s.getStateJson('messages'));
export const getStateJsonAt = cold('getStateJsonAt', (s, f) => s.getStateJsonAt('mode', f.at));
export const getStateLen = cold('getStateLen', s => s.getStateLen('messages'));
export const getStateSlice = cold('getStateSlice', s => utf8(s.getStateSlice('messages', 1, 2)));
export const getStateTail = cold('getStateTail', s => utf8(s.getStateTail('messages', 2)));
export const listBranches = cold('listBranches', branchNames);
export const listStates = cold('listStates', stateList);
export const queryStateIndexEq = cold('queryStateIndexEq', s => s.queryStateIndexEq('messages', '/channel', 'general', {}));
export const queryStateIndexRange = cold('queryStateIndexRange', s => s.queryStateIndexRange('messages', '/n', { gte: 1, lte: 2 }));
export const stateHeadOffset = cold('stateHeadOffset', s => s.stateHeadOffset('messages'));
export const treeGet = cold('treeGet', s => s.treeGet('files', 'a.txt'));
export const treeList = cold('treeList', files);
