#!/usr/bin/env python3
"""Scan a chronicle store for delta-snapshot poisoning on append-log states.

Background (see commits a9677a1 + 5f270d0): before the fix, a DeltaSnapshot
written after a Set (or Edit/Redact that the head metadata lost track of)
walked past the replacement and durably captured resurrected pre-Set items.
The fixed code prevents NEW poisoned deltas but does not heal chains that
already contain one — and the next full Snapshot bakes the resurrection into
the base state permanently.

This scanner is READ-ONLY. For every state chain in records.log it computes:

  as-code  — the state exactly as chronicle reconstructs it (base full
             Snapshot + DeltaSnapshots + trailing raw ops), and
  raw-truth — a replay of raw ops only (Append/Set/Edit/Redact/Delta),
             ignoring every Snapshot/DeltaSnapshot, which are purely derived.

A mismatch means the chain is poisoned (by a bad delta, or by a full
snapshot that baked a bad delta in). It also reports suspect windows
(Set/Edit/Redact followed by a DeltaSnapshot with no full Snapshot between)
even when the head state happens to currently match.

Usage:
    scan-delta-poison.py <store-dir-or-records.log> [--json] [--verbose]

Exit codes: 0 = all clean, 1 = poisoned chain(s) found, 2 = usage/IO error.
"""
import sys, os, struct, json, datetime

RAW_OPS = ("Append", "Set", "Edit", "Redact", "Delta")
NON_APPEND_RAW = ("Set", "Edit", "Redact", "Delta")
TREE_OPS = ("TreeSet", "TreeRemove", "TreeBatch", "TreeDeltaSnapshot")


def parse_log(path):
    """Parse records.log; return {offset: update_dict} for state_update records."""
    records = {}
    with open(path, "rb") as f:
        size = f.seek(0, 2)
        f.seek(0)

        def need(n):
            b = f.read(n)
            if len(b) != n:
                raise EOFError()
            return b

        while f.tell() < size:
            start = f.tell()
            try:
                if need(4) != b"REC\0":
                    break
                need(2)
                rid, seq, br, ts = struct.unpack("<QQQq", need(32))
                tlen = struct.unpack("<H", need(2))[0]
                rtype = need(tlen).decode()
                need(1)
                plen = struct.unpack("<I", need(4))[0]
                payload = need(plen)
                cbc = struct.unpack("<H", need(2))[0]
                need(8 * cbc)
                ltc = struct.unpack("<H", need(2))[0]
                need(8 * ltc)
                need(4)
            except EOFError:
                break  # torn tail on a live store - scan what is durable
            if rtype != "state_update":
                continue
            upd = json.loads(payload)
            upd["_offset"] = start
            upd["_seq"] = seq
            upd["_ts"] = ts
            records[start] = upd
    return records


def op_name(op):
    return op if isinstance(op, str) else next(iter(op))


def op_data(op, name):
    """Decode a Vec<u8> payload (JSON array of ints) into a Python value."""
    raw = op[name]
    return json.loads(bytes(raw)) if raw else None


def apply_raw(state, op):
    """Replay one raw op with chronicle's apply_operation semantics."""
    name = op_name(op)
    if name == "Set":
        return op_data(op, "Set")
    if name == "Delta":
        return json.loads(bytes(op["Delta"]["new_value"]))
    if name == "Append":
        arr = state if isinstance(state, list) else ([] if state is None else state)
        arr.append(op_data(op, "Append"))
        return arr
    if name == "Edit":
        idx = op["Edit"]["index"]
        if isinstance(state, list) and idx < len(state):
            state[idx] = json.loads(bytes(op["Edit"]["new_value"]))
        return state
    if name == "Redact":
        if isinstance(state, list):
            s = min(op["Redact"]["start"], len(state))
            e = min(op["Redact"]["end"], len(state))
            if s < e:
                del state[s:e]
        return state
    return state


def walk_chain(records, head_offset):
    """Follow prev_update_offset links from head; return ops oldest-first."""
    chain = []
    off = head_offset
    while off is not None:
        rec = records.get(off)
        if rec is None:
            break  # prev points before the durable log (trimmed) - partial chain
        chain.append(rec)
        off = rec.get("prev_update_offset")
    chain.reverse()
    return chain


def reconstruct_as_code(chain):
    """Mimic StateManager::reconstruct_from_disk on this chain."""
    collected = []
    hit_snapshot = False
    for rec in reversed(chain):
        name = op_name(rec["operation"])
        if name == "Snapshot":
            collected.append(rec["operation"])
            break
        if name in ("DeltaSnapshot", "TreeDeltaSnapshot"):
            collected.append(rec["operation"])
            hit_snapshot = True
        elif not hit_snapshot:
            collected.append(rec["operation"])
    collected.reverse()
    state = None
    for op in collected:
        name = op_name(op)
        if name == "Snapshot":
            state = op_data(op, "Snapshot")
        elif name == "DeltaSnapshot":
            base = state if isinstance(state, list) else []
            base.extend(op_data(op, "DeltaSnapshot") or [])
            state = base
        else:
            state = apply_raw(state, op)
    return state


def reconstruct_raw_truth(chain):
    """Replay raw ops only, ignoring all (derived) snapshots."""
    state = None
    for rec in chain:
        if op_name(rec["operation"]) in RAW_OPS:
            state = apply_raw(state, rec["operation"])
    return state


def suspect_windows(chain):
    """(non_append_offset, delta_offset) pairs with no full Snapshot between."""
    windows = []
    pending = None  # most recent unsnapshotted non-append raw op
    for rec in chain:
        name = op_name(rec["operation"])
        if name == "Snapshot":
            pending = None
        elif name in NON_APPEND_RAW:
            pending = rec
        elif name == "DeltaSnapshot" and pending is not None:
            windows.append((pending, rec))
            pending = None  # report each window once
    return windows


def open_window(chain):
    """Non-append raw op after the last full Snapshot with no delta yet."""
    pending = None
    for rec in chain:
        name = op_name(rec["operation"])
        if name in ("Snapshot", "DeltaSnapshot"):
            pending = None
        elif name in NON_APPEND_RAW:
            pending = rec
    return pending


def fmt_ts(us):
    return datetime.datetime.utcfromtimestamp(us / 1e6).strftime("%Y-%m-%d %H:%M:%S")


def first_divergence(a, b):
    if not isinstance(a, list) or not isinstance(b, list):
        return None
    for i in range(min(len(a), len(b))):
        if a[i] != b[i]:
            return i
    return min(len(a), len(b)) if len(a) != len(b) else None


def main():
    argv = [a for a in sys.argv[1:] if not a.startswith("--")]
    as_json = "--json" in sys.argv
    verbose = "--verbose" in sys.argv
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    path = argv[0]
    if os.path.isdir(path):
        path = os.path.join(path, "records.log")
    if not os.path.isfile(path):
        print(f"error: {path} not found", file=sys.stderr)
        return 2

    records = parse_log(path)

    # Heads = state_update records that nothing points back to (per branch fork).
    referenced = {
        rec.get("prev_update_offset")
        for rec in records.values()
        if rec.get("prev_update_offset") is not None
    }
    heads = [off for off in records if off not in referenced]

    results = []
    poisoned = 0
    for head in sorted(heads):
        chain = walk_chain(records, head)
        state_id = chain[-1]["state_id"]
        names = [op_name(r["operation"]) for r in chain]

        if any(n in TREE_OPS for n in names):
            results.append({"state_id": state_id, "head": head, "status": "SKIPPED_TREE",
                            "ops": len(chain)})
            continue
        if records.get(chain[0].get("prev_update_offset")) is None and chain[0].get("prev_update_offset") is not None:
            # chain root fell off a trimmed log - raw replay would be incomplete
            results.append({"state_id": state_id, "head": head, "status": "PARTIAL_CHAIN",
                            "ops": len(chain)})
            continue

        try:
            as_code = reconstruct_as_code(chain)
            truth = reconstruct_raw_truth(chain)
        except Exception as e:  # unparseable payloads etc.
            results.append({"state_id": state_id, "head": head, "status": "ERROR",
                            "ops": len(chain), "error": str(e)})
            continue

        windows = suspect_windows(chain)
        pending = open_window(chain)
        entry = {
            "state_id": state_id,
            "head": head,
            "ops": len(chain),
            "deltas": names.count("DeltaSnapshot"),
            "snapshots": names.count("Snapshot"),
            "suspect_windows": [
                {
                    "op": op_name(w[0]["operation"]),
                    "op_offset": w[0]["_offset"],
                    "op_time": fmt_ts(w[0]["_ts"]),
                    "delta_offset": w[1]["_offset"],
                    "delta_time": fmt_ts(w[1]["_ts"]),
                }
                for w in windows
            ],
        }
        if as_code != truth:
            poisoned += 1
            entry["status"] = "POISONED"
            div = first_divergence(as_code, truth)
            entry["as_code_len"] = len(as_code) if isinstance(as_code, list) else None
            entry["truth_len"] = len(truth) if isinstance(truth, list) else None
            entry["first_divergent_index"] = div
            if verbose:
                entry["as_code"] = as_code
                entry["raw_truth"] = truth
        elif windows:
            entry["status"] = "HEALED_BUT_HAD_WINDOW"  # matched at head; inspect anyway
        elif pending is not None:
            entry["status"] = "OPEN_WINDOW"  # safe on fixed code; poisonable on old code
            entry["open_window_op"] = op_name(pending["operation"])
            entry["open_window_time"] = fmt_ts(pending["_ts"])
        else:
            entry["status"] = "CLEAN"
        results.append(entry)

    if as_json:
        print(json.dumps({"log": path, "chains": results}, indent=2))
    else:
        for r in results:
            line = f"[{r['status']:<22}] {r['state_id']}  head@{r['head']}  ops={r['ops']}"
            if r["status"] == "POISONED":
                line += (f"  as_code_len={r.get('as_code_len')} truth_len={r.get('truth_len')}"
                         f" first_divergence@{r.get('first_divergent_index')}")
            if r.get("suspect_windows"):
                for w in r["suspect_windows"]:
                    line += (f"\n    window: {w['op']}@{w['op_offset']} ({w['op_time']})"
                             f" -> DeltaSnapshot@{w['delta_offset']} ({w['delta_time']})")
            if r["status"] == "OPEN_WINDOW":
                line += f"\n    open: {r['open_window_op']} at {r['open_window_time']} with no snapshot after it yet"
            print(line)
        n = len(results)
        print(f"\n{n} chain(s): {poisoned} poisoned, "
              f"{sum(1 for r in results if r['status'] == 'CLEAN')} clean")

    return 1 if poisoned else 0


if __name__ == "__main__":
    sys.exit(main())
