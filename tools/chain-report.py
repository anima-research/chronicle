#!/usr/bin/env python3
import sys, struct, zlib, json, datetime
logp = sys.argv[1]
f = open(logp,'rb'); size=f.seek(0,2); f.seek(0)
def need(n):
    b=f.read(n)
    if len(b)!=n: raise EOFError()
    return b
chains=[]; headpos={}
while f.tell()<size:
    start=f.tell()
    try:
        if need(4)!=b'REC\0': break
        need(2); rid,seq,br,ts=struct.unpack('<QQQq',need(32))
        tlen=struct.unpack('<H',need(2))[0]; rtype=need(tlen).decode(); need(1)
        plen=struct.unpack('<I',need(4))[0]; payload=need(plen)
        cbc=struct.unpack('<H',need(2))[0]; need(8*cbc)
        ltc=struct.unpack('<H',need(2))[0]; need(8*ltc); need(4)
    except EOFError: break
    if rtype!='state_update': continue
    upd=json.loads(payload)
    if upd['state_id']!='messages': continue
    op=upd['operation']; opname=op if isinstance(op,str) else next(iter(op))
    prev=upd.get('prev_update_offset')
    ch = headpos.pop(prev,None) if prev is not None else None
    if ch is None:
        ch={'first_ts':ts,'ops':0,'appends':0,'root':start}
        chains.append(ch)
    ch['ops']+=1
    if opname=='Append': ch['appends']+=1
    ch['last_ts']=ts; ch['head']=start
    headpos[start]=ch
def fmt(us): return datetime.datetime.utcfromtimestamp(us/1e6).strftime('%m-%d %H:%M')
for i,c in enumerate(chains):
    print(f"chain{i}: ops={c['ops']} appends={c['appends']} {fmt(c['first_ts'])} -> {fmt(c['last_ts'])} head@{c['head']}")
