#!/usr/bin/env python3
"""Capture a small register history from an HTTP endpoint."""
import argparse, json, time, urllib.request

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('url'); ap.add_argument('--value',type=int,default=1); ap.add_argument('--output',required=True); a=ap.parse_args()
    ops=[]
    def call(op, value=None):
        ident=f'op-{len(ops)+1}'; start=time.monotonic_ns()
        req=urllib.request.Request(a.url, method='GET' if op=='read' else 'POST', data=None if op=='read' else json.dumps({'value':value}).encode(), headers={'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=10) as r: body=r.read().decode(); observed=value if op=='write' else json.loads(body).get('value')
        finally: end=time.monotonic_ns()
        ops.append({'id':ident,'op':op,'value':observed,'start':start,'end':end})
    call('write', a.value); call('read')
    with open(a.output,'w',encoding='utf-8') as f: json.dump(ops,f,indent=2)
if __name__=='__main__': main()
