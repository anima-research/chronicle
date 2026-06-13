# RFC: State-item origin tracking

**Status**: Draft / design discussion

**Author**: written up after debugging a real bug; see "Motivating bug" below

## What this is about

We want to give callers of an `AppendLog` state slot a way to ask: **"which chronicle record produced each item in this slot?"**

Today there's no answer to that question. The only workaround is for the caller to bake the answer into each item's payload at write time, which forces a two-write pattern (the item, then a patch carrying the record's id and sequence). That doubles chronicle's record count and creates subtle correctness bugs around branching.

This doc walks the design space. None of the approaches is free; the question is which trade-off is the right fit for chronicle.

## Vocabulary

- **Record**: chronicle's lowest-level unit. Has `id`, `sequence`, `branch`, `record_type`, `payload`, etc. Immutable. Append-only.
- **State slot**: a named container layered on top of records. Examples: `messages`, `agents/lena/autobio:summaries`. Each slot has a *strategy* (`Snapshot`, `Delta`, `AppendLog`, `Tree`, `Struct`) that determines how its current value is reconstructed from records.
- **AppendLog slot**: a slot that holds an ordered array of items. Supports `Append`, `Edit`, `Redact`. Uses incremental snapshots to keep reads fast at scale.
- **Origin record** (or just "origin"): for an item currently in an `AppendLog` slot at index N, the chronicle record whose `Append` operation put it there. Edits to the item don't change its origin.

A slot's items today are accessible via `get_state` / `get_state_slice` / `get_state_tail` — payload bytes only. Each item's origin record is *recoverable in principle* (walk records, replay operations, note which record's Append produced each item) but not exposed by any API.

## Motivating bug

Real bug we just fixed in `MessageStore` (the context-manager package):

`MessageStore.append()` does this:

```typescript
const record = store.appendToStateJson(stateId, partialMessage);
// record.id and record.sequence are known now, but they're NOT in the stored payload yet
// — we built partialMessage before the append.

const fullMessage = { id: record.id, sequence: record.sequence, ...partialMessage };
store.editStateItem(stateId, index, fullMessage);
// Patches the just-appended item to embed its own id/sequence. Creates a SECOND record.
```

The doubling matters for three reasons:

1. **Chronicle log size doubles.** A 1M-message conversation produces 2M chronicle records. Snapshot intervals trigger twice as often. Cold-start replay walks twice as much.
2. **Branching is fragile.** `branchAt(messageId)` forks at the message's stored sequence. The naive choice is the *first* record's sequence (the original append) — but at that point the patch hasn't run yet, so the fork-point message has `id`/`sequence` = `undefined` on the new branch. We worked around this by storing the *edit* record's sequence and adding a `+1` invariant comment; it works but is leaky.
3. **The pattern is forced on every caller** that needs to know an item's origin. The MessageStore is just the most prominent case.

## Design constraint

Chronicle's stated design supports many millions of records with **O(1) tail-focused operations**. Any solution that walks records on read invalidates that.

So whatever we do, origin queries need to be O(1) for tail reads (modulo the usual snapshot replay), like content reads are today.

## Approaches

### Approach 1: Walk records on demand

Add `getStateOrigins(stateId)` that walks chronicle records from genesis (skipping the snapshot fast-forward), maintaining origins through Append/Edit/Redact ops, and returns the resulting array.

- **Cost on call**: O(N), where N = total records ever appended to this slot. Cache the result, but cache invalidates on any new write.
- **Cost on memory**: ~16–24 B per cached item.
- **Backward compat**: zero. Works on any existing AppendLog slot.

**Verdict**: violates the O(1) tail-read constraint outright. The cache helps only if writes are rare relative to reads, which is not the typical case. Doesn't fit chronicle's design ethos. Reject.

### Approach 2: Track origins through reconstruction; extend snapshot format

Modify the AppendLog reconstruction logic so it maintains a parallel `origins: Vec<RecordRef>` alongside the items array. Each operation handles origins symmetrically with items:

- `Append(payload)` → push `RecordRef::of(current_record)` onto origins
- `Edit { index, .. }` → origins unchanged
- `Redact { start, end }` → drain origins[start..end] same as items

Snapshots need to preserve origins, otherwise origin tracking breaks across the snapshot boundary (defeating the O(1) tail-read property). Two ways:

- **2a**: Add new variants `SnapshotWithOrigins { state, origins }` and `DeltaSnapshotWithOrigins { delta, origins }`. Old `Snapshot(state)` / `DeltaSnapshot(state)` variants remain readable. The serde discriminator handles forward/back compat.
- **2b**: Make the existing variants struct-shaped with an optional origins field. Slightly more disruption to existing code paths.

The cache grows: each cached slot value now includes `(items_bytes, origins_vec)` instead of just `items_bytes`.

**Backward compat**: old AppendLog slots have `Snapshot(state)` records without origins. When reconstruction hits one of those, it loses origin info for everything in the snapshot. Subsequent appends (after the chronicle upgrade) get tracked; older items don't. `getStateOrigins` returns either `Vec<Option<RecordRef>>` (explicit None for buried items) or `None` for the whole call.

**Sub-decision**: opt-in or default?

- **2-default**: every AppendLog slot pays the origin-tracking cost. ~16–24 B × items, for every slot, whether the caller cares or not. For a slot with 1M items: 16–24 MB extra cache footprint.
- **2-opt-in**: add `embedRecordRef: bool` (or similar) to the AppendLog strategy. Slots that opt in track origins; slots that don't pay nothing. Registering a slot is a one-shot decision; toggling later requires either (a) hard error, (b) accepting that pre-toggle items have no origin info.

**Cost summary**: data format change (acceptable with new variants), memory cost (mitigatable via opt-in), reconstruction code changes.

### Approach 3: Embed origin in the item at write time

Don't track origins separately. Instead, at write time, wrap each `Append`'s payload with the record's own id/sequence so it's recoverable directly from the stored bytes.

- Caller does `appendToStateJson(stateId, item)` as today.
- Chronicle, inside the write lock, knows the to-be-assigned record id and sequence. It serializes the on-disk payload as `{ recordId, sequence, item: <user_payload> }` rather than just `<user_payload>`.
- Reads via a new helper return `{ recordId, sequence, item }` per slot entry.

Implementation choices:

- **3a — JS callback**: caller passes a function `(recordId, sequence) => finalPayload`. Chronicle invokes it inside the lock. Most flexible. NAPI plumbing for threadsafe functions is more complex than value-passing.
- **3b — automatic wrapping**: chronicle just wraps the payload itself. Simpler to implement and use. The wrapper format becomes part of the slot's on-disk contract — items can't be mixed wrapped and unwrapped in one slot.

**Where it costs**:
- Each item carries ~16–24 extra bytes on disk.
- Snapshots (which serialize the full state) inherit those extra bytes naturally — no special snapshot variant needed.
- Reconstruction logic doesn't change — the operations stay simple.

**Backward compat**: slots that already exist don't have wrapped items. The new API only works on slots that opted in (via a new strategy flag, similar to Approach 2). Old slots don't change.

### Approach 4: Hybrid — Approach 3b for new slots, current patching for legacy

Same as Approach 3b but explicit about it: existing callers (MessageStore today) keep the patching dance for back-compat with stores that pre-date the feature. New slots — and slots whose owners explicitly migrate — opt in via the strategy flag and use the new API.

For MessageStore specifically: detect the strategy at init. If it's `AppendLog { embedRecordRef: true }`, use the new direct API. If it's plain `AppendLog`, fall back to the patching dance (with the bug-fix comments already in place).

## Trade-off summary

| | Approach 1 | Approach 2 (opt-in) | Approach 3b/4 |
|---|---|---|---|
| **O(1) tail reads** | No | Yes | Yes |
| **Records per append** | 1 (no doubling) | 1 (no doubling) | 1 (no doubling) |
| **Reconstruction code changes** | Minimal | Moderate | None |
| **Data format change** | None | New StateOperation variants | New AppendLog strategy flag; wrapper format per slot |
| **Memory cost** | Cache as needed | ~16–24 B × items for opted-in slots | None at runtime (cost is on disk) |
| **Disk cost** | None | None (origins replace nothing) | ~16–24 B × items for opted-in slots |
| **Snapshot interaction** | Defeats it | Requires variant extension | Transparent (wrapping is item-level) |
| **Per-slot opt-in** | n/a (always available) | Yes, via strategy field | Yes, via strategy field |
| **Backward compat** | Total (old slots work) | Old snapshots lose origin info | Old slots untouched; new flag is additive |
| **Risk surface** | Performance pitfall | Reconstruction bugs; snapshot serde bugs | On-disk format coupling per slot |

## Decision points (the actual dilemma)

1. **Where does the cost live: memory or disk?**  
   Approach 2 stores origins in the cache (runtime memory). Approach 3 stores them as bytes inside each item (disk + cache, since the cache holds the slot's bytes). For chronicle's "millions of records" target, disk is cheaper but inflates snapshot blob sizes. Memory is cheaper per-item but scales with concurrently-cached slots.

2. **How much can the AppendLog op model shift?**  
   Approach 2 changes how operations are interpreted (Append now produces an origin entry). Approach 3 doesn't touch operations — the wrapping happens BEFORE the op is written. Approach 3 is the smaller blast radius for the AppendLog code.

3. **Snapshot format change: stomach-able?**  
   Approach 2 needs new Snapshot variants. The variants are additive (old variants still readable). Approach 3 doesn't touch snapshots at all.

4. **What about pre-existing AppendLog slots that callers want to migrate?**  
   Hardest under any approach. Old items in old snapshots have no origin info recorded. Options:
   - Error on opt-in if the slot already has items
   - Accept partial coverage (new items have origins, old items return None)
   - Require explicit migration command (e.g., walk old records, rewrite as a new slot)

5. **NAPI shape: minimal `{recordId, sequence}` or full `JsRecord`?**  
   Minimal is cheaper to serialize and matches what callers need (chronicle.id + chronicle.seq are the only fields that aren't in the item payload). Full record preserves API uniformity but forces extra fetches.

## A bias, since you'd ask

If I had to pick: **Approach 4 (Approach 3b + hybrid back-compat)**. Smallest blast radius — wrapping is a per-slot, per-write concern; reconstruction code stays simple; snapshots inherit the property for free; old slots are untouched. The disk overhead is roughly equal to what MessageStore's current patching dance writes anyway (the patch's payload is `{id, sequence, ...partialItem}`), but achieved in one record instead of two.

The cost is that the wrapper becomes part of the slot's on-disk format, which is a minor data-model coupling. The benefit is that the reconstruction algebra stays simple, snapshots Just Work, and migrations are explicit per-slot rather than data-wide.

If we expect lots of slots to need origins (not just `messages` and similar), Approach 2 becomes more attractive — origin tracking becomes a primitive of the AppendLog state machine, available to anyone who opts in. But for the current use case (one slot, MessageStore) Approach 4 is the lighter touch.

## Open questions

- Are there other current or planned slot strategies that would also want origin info? (Tree slots, for example?)
- Is there appetite for the disk-format coupling in Approach 3/4? Specifically: people who download a chronicle file and read it with external tooling would need to know about the wrapping convention for opted-in slots.
- For the snapshot variant in Approach 2: do we want a single `SnapshotV2 { state, origins: Option<Vec<RecordRef>> }` (origins optional in the new variant, easier deprecation path for legacy slots), or strictly two variants?

---

*Written after fixing a real `MessageStore` bug that surfaced this entire problem; the fix landed as a workaround using the +1 trick. This doc is for what would let us delete that workaround.*

---

## Addendum (2026-06-04): findings from re-investigating the motivating bug

Revisited the actual code before committing to any approach. Three findings change how this RFC should be scoped.

**1. The origin *sequence* was already being captured — and then discarded.**
The append dance does store the origin in each item's payload: `editStateItem(..., { id: record.id, sequence: record.sequence, ... })`. The problem was never that the origin couldn't be recovered; it was that `MessageStore.internalToStored` overwrote it on read:

```ts
sequence: index, // Use index as sequence for now
```

So every read returned the array index in place of the embedded chronicle sequence. The data this RFC proposes to add was present all along, one line away from being surfaced. This reframes the premise: for the `messages` slot we don't need a *new* origin-tracking primitive to recover origins — we needed to stop throwing the recovered value away.

**2. `id`-from-record and `sequence`-from-record are separable, and only one is load-bearing.**
The embedded `id` is consumed (it backs `idToIndex`). The embedded `sequence` was dead until now. The id is only entangled with chronicle because we chose `MessageId === RecordId` — which, separately, buys an **O(1)** `getRecord(messageId)` lookup (chronicle indexes records by id in a `HashMap<RecordId, u64>`). That property is worth keeping, and it's the strongest argument *for* in-lock wrapping (Approach 3b/4): wrapping preserves `MessageId === RecordId` while collapsing the two writes into one. Worth stating explicitly — the original draft never noted that the id↔record lookup is already O(1).

**3. The bug that actually bit was branching, and it was a read-side bug, not a chronicle gap.**
`branchAt(messageId)` passed the (index-valued) `sequence` to `createBranchAt`, which expects a global chronicle sequence — so it cut the store near genesis and the branch lost almost all history. `fork()` and `agent-framework`'s `undoLastTurn()` were fine, because they branch at `currentSequence()` (the live head) and never have to map a historical item back to a record. Only "branch at a *past* item" needs the origin sequence.

Fixed in `context-manager` without any chronicle change:
- `internalToStored` surfaces the embedded origin sequence for *display* reads, falling back to the array index only for partial items (interrupted writes). This lenient value must not drive decisions — see next point.
- A strict `MessageStore.originSequence(index)` accessor returns the embedded origin or **throws** if it is absent. Decision paths (branching/forking/time-travel) use this, never the lenient display value, so a damaged store fails loudly instead of silently cutting near genesis.
- `branchAt` now cuts just before the *next* message's origin record (`originSequence(idx+1) - 1`, or the live head if the target is the tail), so the boundary message is included fully formed. Cutting at the target's own append sequence would land before its embedding edit and leave it partial — this is exactly the "+1 invariant" the original write-up disliked, now avoided structurally rather than with an offset.
- Regression tests: `test/branch-at.test.ts` — fails pre-fix (branch comes back near-empty), passes post-fix; plus a guard test asserting a partial boundary item makes `branchAt` throw rather than silently cut near genesis.

**Compatibility:** zero on-disk change. No Rust, record, snapshot, or WAL format change; existing stores open and replay identically, no migration. The only change is the TS read-side meaning of `StoredMessage.sequence` (array index → embedded origin sequence). Surveyed consumers: `branchAt` (fixed) and the adaptive composite (propagates, fine); no in-tree caller treats it as a contiguous index. Legacy items lacking an embedded sequence fall back to the index.

### What this means for the RFC

The motivating bug is resolved at the caller, so **none of Approaches 1–4 are required to fix it.** What remains genuinely motivated:

- **The doubling** (2 records/append) is still real and still worth removing. **Approach 3b/4 (in-lock wrapping)** is the right tool *specifically for that*: one write, preserves `MessageId === RecordId` and its O(1) lookup, and as a side effect makes the origin sequence exact (single record ⇒ no append-vs-edit boundary question, the `branchAt` `next.sequence - 1` dance becomes unnecessary). This is a stronger, narrower case than the original framing.
- **Approach 2 (origins as an AppendLog primitive)** only earns its keep if *multiple* slots need read-time origins for items they didn't write the id into. No such consumer exists today (`chatperx` epic-state reconstructs via its own per-message event log; the autobiographical strategy maps by id, not origin). Until one does, Approach 2 is speculative.

Recommendation, revised: **don't adopt anything for origin *recovery* — it's solved.** Treat the open question as purely "do we want to kill the append doubling?" If yes, do Approach 3b/4 and let exact origins fall out for free. If the doubling is tolerable, the per-item-origin machinery can wait until a second consumer actually needs it.
