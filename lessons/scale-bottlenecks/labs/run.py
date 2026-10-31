import argparse,concurrent.futures,gzip,json,socket,statistics,subprocess,tempfile,threading,time,urllib.request
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--collector',required=True);p.add_argument('--evidence',required=True);a=p.parse_args();binary=str(Path(a.collector).resolve());assert subprocess.check_output([binary,'--version'],text=True).strip()=='otelcol-contrib version 0.163.1'
root=Path(__file__).resolve().parent;e=Path(a.evidence).resolve();e.mkdir(parents=True,exist_ok=False);checks=[];summary={}
def check(label,condition):
 assert condition,label;checks.append(label);(e/'results.data').write_text(json.dumps({'collector':'0.163.1','passed':len(checks),'checks':checks},indent=2)+'\n')
def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
payload=json.loads((root/'input.data').read_text())['requests'];record=payload[0];record['resourceSpans'][0]['scopeSpans'][0]['spans']+=payload[1]['resourceSpans'][0]['scopeSpans'][0]['spans'];data=json.dumps(record).encode()
for case,replicas,concurrency in [('one-serial',1,1),('one-concurrent',1,6),('two-concurrent',2,6)]:
 received=[];lock=threading.Lock();active=0;max_active=0
 class Destination(BaseHTTPRequestHandler):
  def do_POST(self):
   global active,max_active
   body=self.rfile.read(int(self.headers.get('Content-Length','0')))
   if self.headers.get('Content-Encoding')=='gzip':body=gzip.decompress(body)
   row=json.loads(body)
   with lock:
    active+=1;max_active=max(max_active,active);time.sleep(.05);received.append(row);active-=1
   self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(b'{}')
  def log_message(self,*unused):pass
 server=ThreadingHTTPServer(('127.0.0.1',0),Destination);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
 with tempfile.TemporaryDirectory(prefix='collector-bottleneck-') as directory:
  work=Path(directory);owned=[];entries=[]
  try:
   for i in range(replicas):
    entry=port();entries.append(entry);text=(root/'collector.yaml').read_text().replace('@HTTP@',str(entry)).replace('@TARGET@',str(server.server_port));config=work/(str(i)+'.yaml');config.write_text(text)
    valid=subprocess.run([binary,'validate','--config',str(config)],capture_output=True,text=True,timeout=10);(e/(case+'-'+str(i)+'-validate.txt')).write_text(valid.stdout+valid.stderr);check(case+'-'+str(i)+'-valid',valid.returncode==0)
    handle=(work/(str(i)+'.log')).open('w');process=subprocess.Popen([binary,'--config',str(config)],stdout=handle,stderr=handle);owned.append((process,handle,i));deadline=time.monotonic()+10
    while 'Everything is ready' not in (work/(str(i)+'.log')).read_text():
     assert process.poll() is None;assert time.monotonic()<deadline;time.sleep(.03)
   def send(n):
    begin=time.monotonic();request=urllib.request.Request(f'http://127.0.0.1:{entries[n%replicas]}/v1/traces',data=data,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=5) as response:status=response.status
    return status,time.monotonic()-begin
   start=time.monotonic()
   with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:outcomes=list(pool.map(send,range(12)))
   elapsed=time.monotonic()-start;check(case+'-all-input-accepted',all(status==200 for status,latency in outcomes));check(case+'-actual-destination-count',len(received)==12)
   spans=[s for row in received for r in row['resourceSpans'] for scope in r['scopeSpans'] for s in scope['spans']];check(case+'-both-statuses-preserved',sorted(s['status']['code'] for s in spans)==[1]*12+[2]*12);check(case+'-one-destination-worker',max_active==1)
   summary[case]={'collectors':replicas,'caller_concurrency':concurrency,'requests':12,'spans':24,'elapsed_seconds':elapsed,'completed_requests_per_second':12/elapsed,'median_caller_latency_seconds':statistics.median(x[1] for x in outcomes),'destination_service_delay_seconds':.05,'destination_workers':1}
   (e/(case+'-destination.jsonl')).write_text('\n'.join(json.dumps(x) for x in received)+'\n')
  finally:
   for process,handle,i in reversed(owned):
    if process.poll() is None:
     process.terminate()
     try:process.wait(timeout=10)
     except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
    handle.close();(e/(case+'-'+str(i)+'-collector.txt')).write_text((work/(str(i)+'.log')).read_text().replace(str(work),'<WORK>'));check(case+'-'+str(i)+'-stopped',process.returncode==0)
   server.shutdown();server.server_close();thread.join(timeout=2)
check('increased-concurrency-increases-wait',summary['one-concurrent']['median_caller_latency_seconds']>2*summary['one-serial']['median_caller_latency_seconds'])
check('second-collector-does-not-double-controlled-drain',summary['two-concurrent']['completed_requests_per_second']<1.5*summary['one-concurrent']['completed_requests_per_second'])
(e/'observations.data').write_text(json.dumps({'cases':summary,'interpretation':'Bounded observations of an intentionally serial synthetic destination; not Collector CPU saturation, sustainable production throughput, a sizing rule or statistical performance guarantee.'},indent=2)+'\n');print(json.dumps({'passed':len(checks)}))
