"""Run bounded fixtures only against an explicitly selected disposable k3d cluster."""
import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid

ROOT=Path(__file__).resolve().parent
IMAGE='otel/opentelemetry-collector-contrib:0.163.1'
READER='alpine:3.24.2'


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--kubeconfig',required=True,type=Path)
    parser.add_argument('--context',required=True)
    parser.add_argument('--evidence',required=True,type=Path)
    args=parser.parse_args()
    if not args.context.startswith('k3d-dropzone-collector-'):
        parser.error('Use a dedicated disposable k3d-dropzone-collector-* context, never an existing shared cluster')
    args.evidence.mkdir(parents=True,exist_ok=False)
    namespace='collector-lesson-'+uuid.uuid4().hex[:10]
    checks=[]
    created=[]

    def kubectl(*command,data=None,okay=True):
        result=subprocess.run(['kubectl','--kubeconfig',str(args.kubeconfig.resolve()),'--context',args.context,'--request-timeout=25s',*command],input=json.dumps(data) if data is not None else None,capture_output=True,text=True,timeout=100)
        if okay and result.returncode:
            raise AssertionError((command,result.stdout,result.stderr))
        return result

    def check(name,condition,observed):
        if not condition:raise AssertionError((name,observed))
        checks.append({'id':name,'status':'pass','observed':observed})

    def apply(obj):
        obj=copy.deepcopy(obj)
        if obj['kind'] not in ['Namespace','ClusterRole','ClusterRoleBinding']:
            obj.setdefault('metadata',{})['namespace']=namespace
        kubectl('apply','-f','-',data=obj)
        label=obj['metadata']['name']
        created.append(obj)
        return label

    def get(kind,name=None):
        command=['-n',namespace,'get',kind]+([name] if name else [])+['-o','json']
        return json.loads(kubectl(*command).stdout)

    def wait(kind,name):
        if kind=='pod':kubectl('-n',namespace,'wait','pod/'+name,'--for=condition=Ready','--timeout=75s')
        else:kubectl('-n',namespace,'rollout','status',kind+'/'+name,'--timeout=75s')

    def configuration(name,config):
        return apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':name},'data':{'collector.yaml':config,'input.data':(ROOT/'input.data').read_text()}})

    def spec(config,agent=False):
        return {'terminationGracePeriodSeconds':2,'serviceAccountName':'collector','securityContext':{'fsGroup':10001},'containers':[
            {'name':'collector','image':IMAGE,'args':['--config=/conf/collector.yaml'], 'resources':{'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'cpu':'500m','memory':'256Mi'}},'securityContext':{'runAsUser':0 if agent else 10001,'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'env':[{'name':'KUBE_NODE_NAME','valueFrom':{'fieldRef':{'fieldPath':'spec.nodeName'}}}], 'volumeMounts':[{'name':'config','mountPath':'/conf','readOnly':True},{'name':'output','mountPath':'/data'}]+([{'name':'podlogs','mountPath':'/var/log/pods','readOnly':True}] if agent else [])},
            {'name':'reader','image':READER,'command':['sh','-c','sleep 3600'],'resources':{'requests':{'cpu':'5m','memory':'8Mi'},'limits':{'cpu':'100m','memory':'32Mi'}},'volumeMounts':[{'name':'output','mountPath':'/data','readOnly':True},{'name':'config','mountPath':'/fixture','readOnly':True}]}],
            'volumes':[{'name':'config','configMap':{'name':config}},{'name':'output','emptyDir':{}}]+([{'name':'podlogs','hostPath':{'path':'/var/log/pods','type':'Directory'}}] if agent else [])}

    def workload(name,kind,config,agent=False,replicas=1):
        template={'metadata':{'labels':{'app':name}},'spec':spec(config,agent)}
        if kind=='Pod':obj={'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'labels':{'app':name}},'spec':template['spec']}
        else:
            obj={'apiVersion':'apps/v1','kind':kind,'metadata':{'name':name},'spec':{'selector':{'matchLabels':{'app':name}},'template':template}}
            if kind!='DaemonSet':obj['spec']['replicas']=replicas
        apply(obj); wait(kind.lower(),name)
        return [p for p in get('pods')['items'] if p['metadata'].get('labels',{}).get('app')==name]

    def read(pod,filename):
        return kubectl('-n',namespace,'exec',pod,'-c','reader','--','cat','/data/'+filename,okay=False).stdout

    def exported(text,signal):
        return [resource for line in text.splitlines() if line.strip() for resource in json.loads(line).get(signal,[])]

    def send(pod,address):
        return kubectl('-n',namespace,'exec',pod,'-c','reader','--','wget','-qO-','--header=Content-Type:application/json','--post-file=/fixture/input.data',address+'/v1/traces',okay=False)

    def save(name,text):
        text=text.replace(namespace,'<NAMESPACE>')
        text=re.sub(r'collector-lesson-[a-f0-9]+','<NAMESPACE>',text)
        (args.evidence/name).write_text('\n'.join(line.rstrip() for line in text.splitlines()).rstrip()+'\n')

    def base_config(receivers,signal='traces',processors=''):
        return 'receivers:\n'+receivers+'exporters:\n  file:\n    path: /data/output.data\n    flush_interval: 100ms\n'+processors+'service:\n  telemetry:\n    metrics:\n      level: none\n  pipelines:\n    '+signal+':\n      receivers: ['+('file_log' if 'file_log:' in receivers else 'otlp')+']\n'+('      processors: [k8s_attributes]\n' if processors else '')+'      exporters: [file]\n'

    version=json.loads(kubectl('version','-o','json').stdout)
    check('server-version',version['serverVersion']['gitVersion']=='v1.35.5+k3s1',{'server':version['serverVersion']['gitVersion']})
    nodes=json.loads(kubectl('get','nodes','-o','json').stdout)['items']
    check('two-ready-nodes',len(nodes)==2 and all(any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) for n in nodes),'Two dedicated k3d teaching nodes are Ready')
    apply({'apiVersion':'v1','kind':'Namespace','metadata':{'name':namespace}})
    try:
        apply({'apiVersion':'v1','kind':'ServiceAccount','metadata':{'name':'collector'}})
        role={'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':{'name':namespace},'rules':[{'apiGroups':[''],'resources':['nodes','nodes/spec','nodes/stats','namespaces','namespaces/status','pods','pods/status','events','services','replicationcontrollers','replicationcontrollers/status','resourcequotas','persistentvolumes','persistentvolumeclaims','limitranges'],'verbs':['get','list','watch']},{'apiGroups':['apps'],'resources':['daemonsets','deployments','replicasets','statefulsets'],'verbs':['get','list','watch']},{'apiGroups':['batch'],'resources':['jobs','cronjobs'],'verbs':['get','list','watch']},{'apiGroups':['autoscaling'],'resources':['horizontalpodautoscalers'],'verbs':['get','list','watch']},{'apiGroups':['discovery.k8s.io'],'resources':['endpointslices'],'verbs':['get','list','watch']}]}
        apply(role)
        apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRoleBinding','metadata':{'name':namespace},'subjects':[{'kind':'ServiceAccount','name':'collector','namespace':namespace}],'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':namespace}})
        config=configuration('otlp',base_config('  otlp:\n    protocols:\n      http:\n        endpoint: 0.0.0.0:4318\n'))
        gateway=workload('gateway','Deployment',config)[0]
        apply({'apiVersion':'v1','kind':'Service','metadata':{'name':'gateway'},'spec':{'selector':{'app':'gateway'},'ports':[{'port':4318,'targetPort':4318}]}})
        agents=workload('agents','DaemonSet',config)
        sidecar=workload('application-with-sidecar','Pod',config)[0]
        check('one-gateway',get('deployment','gateway')['status']['readyReplicas']==1,'One ready gateway replica')
        check('one-agent-per-node',len(agents)==len(nodes) and len({p['spec']['nodeName'] for p in agents})==len(nodes),{'agents':len(agents),'nodes':len(nodes)})
        check('shared-sidecar-pod',len(sidecar['spec']['containers'])==2 and {c['name'] for c in sidecar['spec']['containers']}=={'collector','reader'},'Synthetic sender and Collector share one Pod/network namespace')
        sender=sidecar['metadata']['name']
        result=send(sender,'http://127.0.0.1:4318')
        check('sidecar-loopback-delivery',result.returncode==0 and 'rejectedSpans' not in result.stdout,{'response':result.stdout.strip()})
        result=send(sender,'http://gateway:4318')
        check('service-gateway-delivery',result.returncode==0 and 'rejectedSpans' not in result.stdout,{'response':result.stdout.strip()})
        time.sleep(.5)
        for label,pod in [('sidecar',sender),('gateway',gateway['metadata']['name'])]:
            text=read(pod,'output.data');resources=exported(text,'resourceSpans')
            spans=[s for r in resources for scope in r['scopeSpans'] for s in scope['spans']]
            expected=json.loads((ROOT/'input.data').read_text())['resourceSpans'][0]['scopeSpans'][0]['spans']
            check(label+'-exact-records',sorted(spans,key=lambda s:s['name'])==sorted(expected,key=lambda s:s['name']),'Both original synthetic spans arrive without field changes')
            save(label+'-traces.data',text)
    finally:
        # Retain only deliberately generated public fixture manifests, never kubeconfig or SA tokens.
        save('fixture-manifests.data',json.dumps(created,indent=2)+'\n')
        for pod in get('pods').get('items',[]):
            for container in pod['spec'].get('containers',[]):
                if 'opentelemetry-collector' in container.get('image',''):
                    logs=kubectl('-n',namespace,'logs',pod['metadata']['name'],'-c',container['name'],okay=False)
                    save(pod['metadata']['name']+'-collector.txt',logs.stdout+logs.stderr)
        status=kubectl('-n',namespace,'get','pods','-o','wide',okay=False)
        save('pod-status.txt',status.stdout+status.stderr)
        kubectl('delete','namespace',namespace,'--wait=true','--timeout=60s',okay=False)
        kubectl('delete','clusterrolebinding',namespace,'--ignore-not-found',okay=False)
        kubectl('delete','clusterrole',namespace,'--ignore-not-found',okay=False)
    result={'collector_image':IMAGE,'kubernetes':version['serverVersion']['gitVersion'],'lesson':ROOT.parent.name,'checks':checks,'fixture_sha256':hashlib.sha256((ROOT/'input.data').read_bytes()).hexdigest(),'limits':'Disposable two-node cluster only; finite fixtures, no production sizing or general exactly-once guarantee'}
    (args.evidence/'results.data').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'lesson':ROOT.parent.name,'passed':len(checks)}))


if __name__=='__main__':main()
