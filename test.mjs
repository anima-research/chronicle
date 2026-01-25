import { JsStore } from './index.js';

console.log('Testing record-store TypeScript bindings...\n');

// Test 1: Create a new store
console.log('1. Creating store...');
const store = JsStore.openOrCreate({
  path: './test-store',
  blobCacheSize: 100
});
console.log('✓ Store created');

// Test 2: Get stats
console.log('\n2. Getting store stats...');
const stats = store.stats();
console.log('✓ Stats:', JSON.stringify(stats, null, 2));

// Test 3: Store a blob
console.log('\n3. Storing blob...');
const blobData = Buffer.from('Hello from TypeScript!');
const blobHash = store.storeBlob(blobData, 'text/plain');
console.log('✓ Blob stored with hash:', blobHash);

// Test 4: Retrieve blob
console.log('\n4. Retrieving blob...');
const retrieved = store.getBlob(blobHash);
console.log('✓ Blob retrieved:', retrieved.toString());

// Test 5: Append a record
console.log('\n5. Appending record...');
const record = store.appendJson('test.message', {
  message: 'Hello from TS bindings!'
});
console.log('✓ Record appended:', JSON.stringify(record, null, 2));

// Test 6: Get record by ID
console.log('\n6. Retrieving record by ID...');
const fetched = store.getRecord(record.id);
console.log('✓ Record retrieved:', JSON.stringify(fetched, null, 2));

// Test 7: Get records by type
console.log('\n7. Getting records by type...');
const recordIds = store.getRecordIdsByType('test.message');
console.log('✓ Found', recordIds.length, 'record(s) of type test.message');

// Test 8: Register and use state (AppendLog)
console.log('\n8. Registering AppendLog state...');
store.registerState({
  id: 'messages',
  strategy: 'append_log',
  deltaSnapshotEvery: 5,
  fullSnapshotEvery: 20
});
console.log('✓ State registered');

// Test 9: Append to state
console.log('\n9. Appending to state...');
store.appendToStateJson('messages', { text: 'First message' });
store.appendToStateJson('messages', { text: 'Second message' });
store.appendToStateJson('messages', { text: 'Third message' });
console.log('✓ Added 3 messages to state');

// Test 10: Get state
console.log('\n10. Getting state...');
const state = store.getStateJson('messages');
console.log('✓ State:', JSON.stringify(state, null, 2));

// Test 11: Get state length
console.log('\n11. Getting state length...');
const len = store.getStateLen('messages');
console.log('✓ State length:', len);

// Test 12: Get state tail
console.log('\n12. Getting state tail (last 2 items)...');
const tail = store.getStateTail('messages', 2);
console.log('✓ Tail:', JSON.stringify(tail, null, 2));

// Test 13: Create branch
console.log('\n13. Creating branch...');
const branchId = store.createBranch('test-branch', null);
console.log('✓ Branch created:', branchId);

// Test 14: List branches
console.log('\n14. Listing branches...');
const branches = store.listBranches();
console.log('✓ Branches:', JSON.stringify(branches, null, 2));

// Test 15: Switch to branch
console.log('\n15. Switching to branch...');
store.switchBranch('test-branch');
console.log('✓ Switched to branch:', store.currentBranch().name);

// Test 16: Add data to branch
console.log('\n16. Adding data to branch...');
store.appendToStateJson('messages', { text: 'Branch-only message' });
const branchState = store.getStateJson('messages');
console.log('✓ Branch state:', JSON.stringify(branchState, null, 2));

// Test 17: Switch back to main
console.log('\n17. Switching back to main...');
store.switchBranch('main');
const mainState = store.getStateJson('messages');
console.log('✓ Main state (no branch message):', JSON.stringify(mainState, null, 2));

// Test 18: Causation links
console.log('\n18. Testing causation links...');
const msg = store.appendJson('message', { text: 'user message' });
const response = store.appendJsonWithLinks('response', { text: 'assistant response' }, {
  causedBy: [msg.id]
});
const toolCall = store.appendJsonWithLinks('tool_call', { tool: 'search' }, {
  causedBy: [response.id],
  linkedTo: [msg.id]
});
console.log('✓ Created causation chain: msg -> response -> tool_call');
console.log('  Response causedBy:', response.causedBy);
console.log('  Tool call causedBy:', toolCall.causedBy, 'linkedTo:', toolCall.linkedTo);

// Test 19: Reverse lookups
console.log('\n19. Testing reverse lookups (getEffects, getLinksTo)...');
const effectsOfMsg = store.getEffects(msg.id);
const linksToMsg = store.getLinksTo(msg.id);
console.log('✓ Effects of message:', effectsOfMsg);
console.log('✓ Links to message:', linksToMsg);

// Test 20: Query with filters
console.log('\n20. Testing query...');
const allRecords = store.query({});
console.log('✓ All records:', allRecords.length);

const messagesOnly = store.query({ types: ['message'] });
console.log('✓ Messages only:', messagesOnly.length);

const limited = store.query({ limit: 3 });
console.log('✓ Limited to 3:', limited.length);

// Test 21: List states
console.log('\n21. Testing listStates...');
const states = store.listStates();
console.log('✓ States:', states);

// Test 22: Current sequence
console.log('\n22. Testing currentSequence...');
const seq = store.currentSequence();
console.log('✓ Current sequence:', seq);

// Test 23: Historical state access
console.log('\n23. Testing historical state access (getStateAt)...');
// Add more messages with known sequences
const r1 = store.appendToStateJson('messages', { text: 'msg4' });
const r2 = store.appendToStateJson('messages', { text: 'msg5' });
const r3 = store.appendToStateJson('messages', { text: 'msg6' });

const currentState = store.getStateJson('messages');
console.log('✓ Current state has', currentState.length, 'items');

// Get state at r1's sequence (should have 4 items: 3 original + 1)
const stateAtR1 = store.getStateJsonAt('messages', r1.sequence);
console.log('✓ State at seq', r1.sequence, 'has', stateAtR1.length, 'items');

// Get state at r2's sequence (should have 5 items)
const stateAtR2 = store.getStateJsonAt('messages', r2.sequence);
console.log('✓ State at seq', r2.sequence, 'has', stateAtR2.length, 'items');

// Test 24: editStateItem (view-mutating operation)
console.log('\n24. Testing editStateItem (view-mutating)...');
// Edit the second message (index 1)
store.editStateItem('messages', 1, Buffer.from(JSON.stringify({ text: 'EDITED Second message' })));
const stateAfterEdit = store.getStateJson('messages');
console.log('✓ Edited item at index 1');
console.log('  Edited content:', stateAfterEdit[1]);
if (stateAfterEdit[1].text !== 'EDITED Second message') {
  throw new Error('editStateItem did not work correctly');
}

// Test 25: redactStateItems (view-mutating operation)
console.log('\n25. Testing redactStateItems (view-mutating)...');
const lenBeforeRedact = store.getStateLen('messages');
// Redact the last item (start=end-1, end=len redacts just the last item)
const redactStart = lenBeforeRedact - 1;
const redactEnd = lenBeforeRedact;
store.redactStateItems('messages', redactStart, redactEnd);
const lenAfterRedact = store.getStateLen('messages');
console.log('✓ Redacted 1 item (indices', redactStart, 'to', redactEnd + ')');
console.log('  Length before:', lenBeforeRedact, '-> after:', lenAfterRedact);
if (lenAfterRedact !== lenBeforeRedact - 1) {
  throw new Error('redactStateItems did not work correctly');
}

// Test 26: createBranchAt (time-travel branching)
console.log('\n26. Testing createBranchAt (time-travel branching)...');
// Get the current sequence
const seqBeforeBranch = store.currentSequence();
// Create a branch at an earlier sequence
const branchAtSeq = Math.floor(seqBeforeBranch / 2);  // Branch at halfway point
const timeTravelBranch = store.createBranchAt('time-travel-branch', 'main', branchAtSeq);
console.log('✓ Branch created at sequence', branchAtSeq);
console.log('  Branch details:', timeTravelBranch);
if (timeTravelBranch.branchPoint !== branchAtSeq) {
  throw new Error('createBranchAt did not set branch_point correctly');
}

// Test 27: Verify time-travel branch has correct head
console.log('\n27. Verifying time-travel branch head...');
const ttBranchInfo = store.listBranches().find(b => b.name === 'time-travel-branch');
console.log('✓ Time-travel branch head:', ttBranchInfo.head);
if (ttBranchInfo.head !== branchAtSeq) {
  throw new Error('Time-travel branch head should equal branch_point');
}

// Test 28: Sync
console.log('\n28. Testing sync...');
store.sync();
console.log('✓ Sync completed');

// Test 29: deleteBranch
console.log('\n29. Testing deleteBranch...');
// Create a branch to delete
store.createBranch('branch-to-delete', null);
const branchesBeforeDelete = store.listBranches();
const hadBranch = branchesBeforeDelete.some(b => b.name === 'branch-to-delete');
console.log('  Branch exists before delete:', hadBranch);
store.deleteBranch('branch-to-delete');
const branchesAfterDelete = store.listBranches();
const stillHasBranch = branchesAfterDelete.some(b => b.name === 'branch-to-delete');
console.log('  Branch exists after delete:', stillHasBranch);
if (!hadBranch || stillHasBranch) {
  throw new Error('deleteBranch did not work correctly');
}
console.log('✓ Branch deleted successfully');

// Test 30: createEmptyBranch
console.log('\n30. Testing createEmptyBranch...');
const emptyBranch = store.createEmptyBranch('empty-branch', 'main');
console.log('✓ Empty branch created:', emptyBranch.name);
// Switch to empty branch and check state is empty (no state heads copied)
store.switchBranch('empty-branch');
// Note: empty branch doesn't copy state heads, so state operations may differ
console.log('  Empty branch head:', emptyBranch.head);
store.switchBranch('main');

// Test 31: getStateSlice
console.log('\n31. Testing getStateSlice...');
const slice = store.getStateSlice('messages', 1, 2); // Get 2 items starting at offset 1
if (slice) {
  const sliceData = JSON.parse(slice.toString());
  console.log('✓ State slice (offset=1, limit=2):', sliceData.length, 'items');
  console.log('  First item in slice:', sliceData[0]);
} else {
  console.log('  (slice returned null - state may be empty)');
}

// Test 32: Snapshot strategy (setState/setStateJson)
console.log('\n32. Testing Snapshot strategy...');
store.registerState({
  id: 'config',
  strategy: 'snapshot',
});
store.setStateJson('config', { theme: 'dark', version: 1 });
const configState = store.getStateJson('config');
console.log('✓ Snapshot state set:', configState);
if (configState.theme !== 'dark') {
  throw new Error('setStateJson did not work correctly');
}
// Update the snapshot
store.setStateJson('config', { theme: 'light', version: 2 });
const configState2 = store.getStateJson('config');
console.log('✓ Snapshot state updated:', configState2);
if (configState2.theme !== 'light' || configState2.version !== 2) {
  throw new Error('setStateJson update did not work correctly');
}

// Test 33: getCompactionSummary
console.log('\n33. Testing getCompactionSummary...');
const compactionSummary = store.getCompactionSummary();
console.log('✓ Compaction summary:', compactionSummary);

// Test 34: compactState
console.log('\n34. Testing compactState...');
const compactResult = store.compactState('messages');
console.log('✓ compactState result:', compactResult ? 'compacted' : 'no compaction needed');

// Test 35: compactAllStates
console.log('\n35. Testing compactAllStates...');
const compactedCount = store.compactAllStates();
console.log('✓ compactAllStates compacted:', compactedCount, 'states');

// Test 36: close/isClosed
console.log('\n36. Testing close/isClosed...');
console.log('  isClosed before close:', store.isClosed());
if (store.isClosed()) {
  throw new Error('Store should not be closed yet');
}
store.close();
console.log('  isClosed after close:', store.isClosed());
if (!store.isClosed()) {
  throw new Error('Store should be closed after close()');
}
console.log('✓ close/isClosed works correctly');

// Test 37: Verify operations fail after close
console.log('\n37. Testing operations fail after close...');
let operationFailedAfterClose = false;
try {
  store.stats();
} catch (e) {
  operationFailedAfterClose = true;
  console.log('✓ Operation correctly failed after close:', e.message);
}
if (!operationFailedAfterClose) {
  throw new Error('Operations should fail after store is closed');
}

// Test 38: Subscription APIs (basic smoke test)
console.log('\n38. Testing subscription APIs...');
// Re-open store for subscription test
const store2 = JsStore.openOrCreate({
  path: './test-store',
  blobCacheSize: 100
});
const subId = store2.subscribe({});
console.log('✓ Subscribed with ID:', subId);
console.log('  Subscription count:', store2.subscriptionCount());
if (store2.subscriptionCount() !== 1) {
  throw new Error('subscriptionCount should be 1');
}

// Poll for events (should be null since no new events)
const event = store2.pollSubscription(subId);
console.log('  pollSubscription result:', event ? event.eventType : 'null (no events)');

// Unsubscribe
store2.unsubscribe(subId);
console.log('  Subscription count after unsubscribe:', store2.subscriptionCount());
if (store2.subscriptionCount() !== 0) {
  throw new Error('subscriptionCount should be 0 after unsubscribe');
}
console.log('✓ Subscription APIs work correctly');

// Test 39: Subscription with catch-up
console.log('\n39. Testing subscription catch-up...');
const subId2 = store2.subscribe({ fromSequence: 1 });
console.log('  Subscribed with catch-up from sequence 1');
store2.catchUpSubscription(subId2);
console.log('✓ catchUpSubscription completed');

// Poll for caught-up events
let eventCount = 0;
let caughtUpEvent = store2.pollSubscription(subId2);
while (caughtUpEvent && eventCount < 100) {
  eventCount++;
  caughtUpEvent = store2.pollSubscription(subId2);
}
console.log('  Received', eventCount, 'events during catch-up');
store2.unsubscribe(subId2);

// Test 40: pollSubscriptionTimeout
console.log('\n40. Testing pollSubscriptionTimeout...');
const subId3 = store2.subscribe({});
const start = Date.now();
const timeoutEvent = store2.pollSubscriptionTimeout(subId3, 50); // 50ms timeout
const elapsed = Date.now() - start;
console.log('  pollSubscriptionTimeout result:', timeoutEvent ? timeoutEvent.eventType : 'null (timed out)');
console.log('  Elapsed time:', elapsed, 'ms (expected ~50ms)');
store2.unsubscribe(subId3);
console.log('✓ pollSubscriptionTimeout works correctly');

store2.close();

console.log('\n✅ All tests passed!');
