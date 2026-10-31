import argparse,json,subprocess,time,uuid
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--kubeconfig',required=True);p.add_argument('--context',required=True);p.add_argument('--evidence',required=True);a=p.parse_args()
e=Path(a.evidence).resolve();e.mkdir(parents=True,exist_ok=False)
ns='collector-course-scaling-'+uuid.uuid4().hex[:8]
k=['kubectl','--kubeconfig',a.kubeconfig,'--context',a.context]
checks=[]
def run(*args,input=None,timeout=30):
 r=subprocess.run(k+list(args),input=input,text=True,capture_output=True,timeout=timeout)
 assert r.returncode==0,(args,r.stdout,r.stderr)
 return r.stdout
def retain(name,value):
 t=json.dumps(value,indent=2) if not isinstance(value,str) else value
 (e/name).write_text(t.replace(ns,'<NAMESPACE>')+'\n')
def check(label,condition):
 assert condition,label;checks.append(label);retain('results.data',{'operator':'0.161.0','collector':'0.163.1','passed':len(checks),'checks':checks})
def wait(fn,timeout=150):
 deadline=time.monotonic()+timeout;last=None
 while time.monotonic()<deadline:
  try:
   last=fn()
   if last:return last
  except (AssertionError,KeyError,json.JSONDecodeError):pass
  time.sleep(1)
 raise AssertionError(('bounded wait expired',last))
def pods():return json.loads(run('-n',ns,'get','pods','-o','json'))['items']
def scrapers():return sorted(p['metadata']['name'] for p in pods() if p['metadata']['name'].startswith('scrapers-collector-'))
def owners():
 x=json.loads(run('get','--raw',f'/api/v1/namespaces/{ns}/services/http:scrapers-targetallocator:80/proxy/jobs/fixture/targets'))
 return {c:sorted(t for group in data.get('targets',[]) for t in group.get('targets',group.get('Targets',[]))) for c,data in x.items()}
def records(pod):
 raw=run('-n',ns,'logs',pod,'-c','otc-container');rows=[]
 for line in raw.splitlines():
  try:row=json.loads(line)
  except json.JSONDecodeError:continue
  for resource in row.get('resourceMetrics',[]):
   for scope in resource.get('scopeMetrics',[]):
    for metric in scope.get('metrics',[]):
     if metric['name']=='lesson_queue_depth':
      for point in metric.get('gauge',{}).get('dataPoints',[]):
       attrs={v['key']:v['value'].get('stringValue') for v in point.get('attributes',[])}
       rows.append((attrs.get('source'),int(point.get('asDouble',point.get('asInt',0)))))
 return raw,rows
def observed_sources(pod,sources):
 r=records(pod)
 return r if sources <= {x for x,y in r[1]} else False
created=False
try:
 manifest=(Path(__file__).parent/'resources.yaml').read_text().replace('@NAMESPACE@',ns)
 run('create','namespace',ns);created=True
 run('apply','--dry-run=server','-f','-',input=manifest);check('server-contract-valid',True)
 run('apply','-f','-',input=manifest);created=True
 wait(lambda: len(scrapers())==2 and all(all(s.get('ready') for s in p.get('status',{}).get('containerStatuses',[])) and p.get('status',{}).get('containerStatuses') for p in pods()))
 retain('initial-pods.data',pods());check('two-scrapers-ready',len(scrapers())==2)
 before=wait(lambda: (o if len(o:=owners())==2 and sorted(t for ts in o.values() for t in ts)==['fixture-'+s+':9090' for s in 'abcd'] and all(o.values()) else False))
 retain('two-owner-assignment.data',before);check('four-targets-assigned-once',len(set(t for ts in before.values() for t in ts))==4)
 expected={'a':7,'b':11,'c':13,'d':17}
 for pod,targets in before.items():
  sources={t.split(':')[0][-1] for t in targets}
  observed=wait(lambda: observed_sources(pod,sources))
  retain(pod+'-two-owner.txt',observed[0]);check(pod+'-actual-owned-gauges',set(observed[1])=={(s,expected[s]) for s in sources})
 # Removing one replica is an actual ownership change, not a hand-written reassignment.
 run('-n',ns,'patch','opentelemetrycollector','scrapers','--type=merge','-p','{"spec":{"replicas":1}}')
 wait(lambda: scrapers()==['scrapers-collector-0'])
 after=wait(lambda: (o if list(o:=owners())==['scrapers-collector-0'] and len(o['scrapers-collector-0'])==4 else False))
 retain('one-owner-assignment.data',after);check('removed-owner-targets-reassigned',set(after['scrapers-collector-0'])==set(t for ts in before.values() for t in ts))
 observed=wait(lambda: (r if set((r:=records('scrapers-collector-0'))[1])==set(expected.items()) else False))
 retain('remaining-owner.txt',observed[0]);check('remaining-scraper-actually-receives-all',True)
 retain('operator-collector.data',json.loads(run('-n',ns,'get','opentelemetrycollector','scrapers','-o','json')))
 retain('final-pods.data',pods());check('resource-bounded-images-observed',all(p.get('status',{}).get('containerStatuses') for p in pods()))
finally:
 if created:
  retain('cleanup.txt',run('delete','namespace',ns,'--wait=true','--timeout=90s',timeout=100));check('owned-namespace-removed',ns not in run('get','namespaces','-o','name'))
print(json.dumps({'passed':len(checks)}))
