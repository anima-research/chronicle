#!/usr/bin/env node
// Print a digest of every state's reconstructed value in a Chronicle store,
// plus record/blob counts, so two stores (or one store before and after
// compact-set-states.py) can be compared with `diff`.
//
// Usage: CHRONICLE_MODULE=<path to @animalabs/chronicle> node state-digests.mjs <store-dir>
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';

const require = createRequire(import.meta.url);
const mod = process.env.CHRONICLE_MODULE;
if (!mod) { console.error('set CHRONICLE_MODULE to the @animalabs/chronicle package directory'); process.exit(2); }
const { JsStore } = require(resolve(mod));
const store = JsStore.open({ path: process.argv[2] });
const sha = (s) => createHash('sha256').update(s).digest('hex').slice(0, 16);
const stats = store.stats();
console.log(`records=${stats.recordCount} blobs=${stats.blobCount} branches=${stats.branchCount} stateSlots=${stats.stateSlotCount}`);
const states = store.listStates().map((s) => s.id).sort();
for (const id of states) {
  let value;
  try { value = store.getStateJson(id); } catch (e) { console.log(`${id}\tERROR ${e.message}`); continue; }
  const text = JSON.stringify(value);
  console.log(`${id}\t${text === undefined ? 'undefined' : sha(text)}\t${text === undefined ? 0 : text.length}`);
}
const last = store.query({ limit: 1, reverse: true })[0];
store.close();
console.log(`lastSeq=${last?.sequence ?? 'none'}`);
