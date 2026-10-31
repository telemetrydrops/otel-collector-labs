import argparse,json,os,re,signal,socket,subprocess,tempfile,time,urllib.request
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--collector',required=True);p.add_argument('--evidence',required=True);a=p.parse_args();binary=str(Path(a.collector).resolve());assert subprocess.check_output([binary,'--version'],text=True).strip()=='otelcol-contrib version 0.163.1'
root=Path(__file__).resolve().parent;e=Path(a.evidence).resolve();e.mkdir(parents=True,exist_ok=False);checks=[];tail=False
def check(label,condition):
 assert condition,label;checks.append(label);(e/'results.data').write_text(json.dumps({'collector':'0.163.1','passed':len(checks),'checks':checks},indent=2)+'\n')
def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def clean(s,work):return re.sub(r'"service.instance.id": "[^"]+"','"service.instance.id": "<INSTANCE>"',s.replace(str(work),'<WORK>'))
def start(work,name,template,replace,owned):
 text=(root/template).read_text()
 for key,value in replace.items():text=text.replace('@'+key+'@',str(value))
 path=work/(name+'.yaml');path.write_text(text);valid=subprocess.run([binary,'validate','--config',str(path)],capture_output=True,text=True,timeout=10);(e/(name+'-validate.txt')).write_text(clean(valid.stdout+valid.stderr,work));check(name+'-valid',valid.returncode==0)
 handle=(work/(name+'.log')).open('w');proc=subprocess.Popen([binary,'--config',str(path)],stdout=handle,stderr=handle);owned.append((name,proc,handle))
 deadline=time.monotonic()+10
 while 'Everything is ready' not in (work/(name+'.log')).read_text():
  assert proc.poll() is None,(work/(name+'.log')).read_text();assert time.monotonic()<deadline,'startup timeout';time.sleep(.03)
def read(path):
 if not path.exists():return []
 spans=[]
 for line in path.read_text().splitlines():
  try:row=json.loads(line)
  except json.JSONDecodeError:continue
  spans.extend(s for resource in row.get('resourceSpans',[]) for scope in resource['scopeSpans'] for s in scope['spans'])
 return spans
def post(endpoint,request):
 req=urllib.request.Request(f'http://127.0.0.1:{endpoint}/v1/traces',data=json.dumps(request).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=5) as response:return response.status
requests=json.loads((root/'input.data').read_text())['requests'];expected={r['resourceSpans'][0]['scopeSpans'][0]['spans'][0]['traceId'] for r in requests}
for case in (['affinity','split'] if tail else ['trace-id','service-key']):
 with tempfile.TemporaryDirectory(prefix='collector-routing-') as directory:
  work=Path(directory);owned=[];g=[port(),port()];h=[port(),port()];entry=port()
  try:
   for i in range(2):start(work,case+'-owner'+str(i),'owner.yaml',{'GRPC':g[i],'HTTP':h[i],'OUTPUT':work/('owner'+str(i)+'.jsonl')},owned)
   if case!='split':start(work,case+'-router',('trace-id' if case=='affinity' else case)+'.yaml',{'HTTP':entry,'OWNER0':g[0],'OWNER1':g[1]},owned)
   statuses=[post(h[j%2] if case=='split' else entry,request) for j,request in enumerate(requests)]
   check(case+'-all-requests-accepted',statuses==[200]*64)
   total=32 if case=='split' else 64;deadline=time.monotonic()+10
   while True:
    spans=[read(work/('owner'+str(i)+'.jsonl')) for i in range(2)]
    if sum(map(len,spans))==total:break
    assert time.monotonic()<deadline,(case,list(map(len,spans)));time.sleep(.1)
   owners={trace:{i for i,rows in enumerate(spans) if any(s['traceId']==trace for s in rows)} for trace in expected}
   check(case+'-no-unexpected-traces',{s['traceId'] for rows in spans for s in rows}==expected)
   check(case+'-one-owner-per-trace',all(len(v)==1 for v in owners.values()))
   if case=='split':
    check(case+'-success-half-lost',not spans[0] and len(spans[1])==32 and all(s['name']=='checkout-error' and s['status']['code']==2 for s in spans[1]))
   else:
    check(case+'-both-original-spans-per-trace',all(sorted(s['name'] for rows in spans for s in rows if s['traceId']==trace)==['checkout-error','checkout-success'] for trace in expected))
    check(case+'-statuses-preserved',sorted(s['status']['code'] for rows in spans for s in rows)==[1]*32+[2]*32)
    check(case+'-distribution-boundary',all(spans) if case!='service-key' else sum(bool(x) for x in spans)==1)
   (e/(case+'-assignment.data')).write_text(json.dumps({'owners':{t:sorted(v) for t,v in owners.items()},'span_counts':list(map(len,spans))},indent=2)+'\n')
  finally:
   for name,proc,handle in reversed(owned):
    if proc.poll() is None:
     proc.send_signal(signal.SIGTERM)
     try:proc.wait(timeout=10)
     except subprocess.TimeoutExpired:proc.kill();proc.wait(timeout=5)
    handle.close();(e/(name+'-collector.txt')).write_text(clean((work/(name+'.log')).read_text(),work));check(name+'-stopped',proc.returncode==0)
   for i in range(2):
    path=work/('owner'+str(i)+'.jsonl')
    if path.exists():(e/(case+'-owner'+str(i)+'.jsonl')).write_bytes(path.read_bytes())
print(json.dumps({'passed':len(checks)}))
