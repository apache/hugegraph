#!/usr/bin/env python3
"""Capture a small register history from an HTTP endpoint."""
import argparse, json, time, urllib.request

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def capture(url, value):
    ops=[]
    def call(op, value=None):
        ident=f'op-{len(ops)+1}'; start=time.monotonic_ns()
        req=urllib.request.Request(url, method='GET' if op=='read' else 'POST', data=None if op=='read' else json.dumps({'value':value}).encode(), headers={'Content-Type':'application/json'})
        with OPENER.open(req, timeout=10) as r: body=r.read().decode(); observed=value if op=='write' else json.loads(body).get('value')
        ops.append({'id':ident,'op':op,'value':observed,'start':start,'end':time.monotonic_ns()})
    call('write', value); call('read'); return ops

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('url'); ap.add_argument('--value',type=int,default=1); ap.add_argument('--output',required=True); a=ap.parse_args()
    ops=capture(a.url, a.value)
    with open(a.output,'w',encoding='utf-8') as f: json.dump(ops,f,indent=2)
if __name__=='__main__': main()
